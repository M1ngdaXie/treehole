package main

import (
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ---- 调参 ----
const (
	writeWait   = 10 * time.Second // 单次写超时
	pongWait    = 60 * time.Second // 多久没收到 pong 判定掉线
	pingPeriod  = 50 * time.Second // 心跳间隔(必须 < pongWait)
	maxMsgBytes = 4096

	ctrlBuf = 8  // 控制信箱缓冲:控制消息低频,8 足够,正常永不溢出
	inBuf   = 32 // 入站信箱缓冲(socket 字节 + 对方聊天)
	outBuf  = 32 // 出站信箱缓冲(写回本连接)

	// 限流:纯标准库令牌桶
	matchRatePerSec = 0.5             // 发起配对/skip:每秒补 0.5 个令牌(≈10 秒 5 次)
	matchBurst      = 5               //   突发上限 5
	msgRatePerSec   = 3.0             // 发消息:每秒补 3 个
	msgBurst        = 10              //   突发上限 10
	ipBucketTTL     = 5 * time.Minute // 不活跃 IP 桶清理阈值
)

// 控制指令:只在「整条消息恰好等于」该命令时识别,带额外文字的仍按聊天转发。
const (
	cmdSkip = "/skip"
	cmdNext = "/next" // 历史别名,等价于 /skip
)

// ───────────────────────── 消息类型 ─────────────────────────

// 控制消息(matchmaker → client),走 ctrl 信道。
type ctrlKind int

const (
	ctrlMatched     ctrlKind = iota // 配上了,partner 指向对方
	ctrlPartnerLeft                 // 对方离开了
)

type ctrlMsg struct {
	kind    ctrlKind
	partner *Client // 仅 ctrlMatched 用
}

// 入站消息(→ client.loop),走 in 信道。
// from == nil 表示来自本人 socket(可能是 /skip 命令或要转发的聊天);
// from != nil 表示来自对方的聊天,需校验 from == 当前 partner 才接收(防串话)。
type inMsg struct {
	from *Client
	data []byte
}

// matchmaker 信箱消息。
type mmKind int

const (
	mmRegister mmKind = iota // 首次加入,找人配
	mmSkip                   // 换一个:解除当前配对并重新找人
	mmGone                   // 连接焚毁:出队 + 通知对方
)

type mmMsg struct {
	kind mmKind
	c    *Client
}

var matchbox = make(chan mmMsg, 256) // matchmaker 唯一入口;FIFO 保证 Gone 先于后续 Register 被处理

// ───────────────────────── Client ─────────────────────────

// Client 是一个连接着的人的 actor。没有任何身份信息,不存历史消息。
// partner 是私有状态,只有本人的 loop() 读写 —— 不需要任何锁。
type Client struct {
	conn    *websocket.Conn
	ip      string        // 限流键
	gender  string        // "male" | "female" | ""
	seeking string        // "opposite" | "same" | "" (空==随便)
	ctrl    chan ctrlMsg  // 控制信箱(matchmaker 投)
	in      chan inMsg    // 入站信箱(自己的 readPump + 对方的聊天)
	out     chan []byte   // 出站信箱(writePump 抽干写回连接)
	done    chan struct{} // 关闭即焚信号,close 一次
	once    sync.Once

	partner *Client // 仅 loop() 访问

	connectTime time.Time // 统计：记录连接建立的时间
}

// shutdown 焚毁本连接:关 done(让三个泵 and loop 退出)、通知 matchmaker 出队、关 conn。
// 幂等(sync.Once)。matchmaker 收到 Gone 后会解除配对并通知对方,因此这里不直接碰对方。
func (c *Client) shutdown() {
	c.once.Do(func() {
		close(c.done)
		// 异步投 Gone:不在 Once 内阻塞;matchmaker 永不阻塞,会很快抽干。
		go func() { matchbox <- mmMsg{kind: mmGone, c: c} }()
		go c.conn.Close() // 关连接可能阻塞在 I/O,放独立 goroutine
	})
}

// sendCtrl 由 matchmaker 调用,非阻塞 —— 中枢永不阻塞,这是全局防死锁的关键。
// ctrl 缓冲足够大且控制消息低频,活跃连接正常不会触发 default;僵死连接丢弃无所谓。
func (c *Client) sendCtrl(m ctrlMsg) {
	select {
	case c.ctrl <- m:
	case <-c.done:
	default:
	}
}

// postIn 把对方的聊天投进本人入站信箱,非阻塞(聊天数据可丢:对方拥塞就丢这条)。
func (c *Client) postIn(m inMsg) {
	select {
	case c.in <- m:
	case <-c.done:
	default:
	}
}

// write 把一帧排进出站信箱;阻塞直到有空位或已焚毁。
// 若连接写端真的卡死,writePump 的 writeWait 截止会触发错误 → shutdown → done 关闭 → 这里经
// <-c.done 解除阻塞。所以不会永久卡住,也不会误杀短暂积压的活跃连接。
func (c *Client) write(b []byte) {
	select {
	case c.out <- b:
	case <-c.done:
	}
}

func (c *Client) sysMsg(kind string) { c.write([]byte("S:" + kind)) }

// ───────────────────────── loop:单 goroutine 串行处理本人全部状态 ─────────────────────────

// loop 优先抽干 ctrl(配对/解除配对的状态必须及时反映),再处理一条入站。
func (c *Client) loop() {
	for {
		// 先尽量清空控制信箱
		select {
		case <-c.done:
			return
		case m := <-c.ctrl:
			c.handleCtrl(m)
			continue
		default:
		}
		select {
		case <-c.done:
			return
		case m := <-c.ctrl:
			c.handleCtrl(m)
		case m := <-c.in:
			c.handleIn(m)
		}
	}
}

func (c *Client) handleCtrl(m ctrlMsg) {
	switch m.kind {
	case ctrlMatched:
		c.partner = m.partner
		c.sysMsg("matched")
	case ctrlPartnerLeft:
		c.partner = nil
		c.sysMsg("partner_left")
	}
}

func (c *Client) handleIn(m inMsg) {
	if m.from != nil {
		// 对方的聊天:只有 from 仍是当前 partner 才接收。
		// 这一句就是「串话 bug」的全部修复 —— 在接收方自己的 goroutine 里原子比对,无锁。
		if m.from == c.partner {
			c.write(append([]byte("M:"), m.data...))
		}
		return
	}

	// 来自本人 socket。
	switch string(m.data) {
	case cmdSkip, cmdNext:
		if !matchLimiter.allow(c.ip) {
			c.sysMsg("rate_limited")
			return
		}
		c.partner = nil // 本地立刻断开,停止再往旧对方投递;matchmaker 负责通知旧对方
		matchbox <- mmMsg{kind: mmSkip, c: c}
		return
	}

	// 普通聊天:过发消息限流后转发给对方。
	if !msgLimiter.allow(c.ip) {
		c.sysMsg("rate_limited")
		return
	}
	p := c.partner
	if p == nil {
		return // 还没配上,忽略
	}
	// 复制一份再带上 from 投给对方;非阻塞。data 是 ReadMessage 返回的独立切片,这里复制以免
	// 与后续读复用缓冲产生别名(gorilla 当前实现每次新分配,复制是保险起见)。
	cp := make([]byte, len(m.data))
	copy(cp, m.data)
	p.postIn(inMsg{from: c, data: cp})
}

// ───────────────────────── 读写泵 ─────────────────────────

func (c *Client) readPump() {
	defer c.shutdown()

	c.conn.SetReadLimit(maxMsgBytes)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		// 投给自己的 loop 决定是命令还是转发;loop 卡住的唯一情形是写端卡死,那时 done 会被关。
		select {
		case c.in <- inMsg{data: raw}:
		case <-c.done:
			return
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.shutdown()
	}()

	for {
		select {
		case <-c.done:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			c.conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		case b := <-c.out:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, b); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ───────────────────────── matchmaker(全局唯一 actor)─────────────────────────

func matchmaker() {
	var waiting []*Client                 // 至多一个等待者
	partners := make(map[*Client]*Client) // 当前配对图,仅本 goroutine 访问

	// 统计核心变量 (由于收拢在单 goroutine 处理，天然无锁竞态)
	totalUsers := 0
	pairStart := make(map[*Client]time.Time) // 维护单次配对的开始时间

	// 内部辅助监控看板
	logStatus := func() {
		log.Printf("[看板] 实时在线总人数: %d | 正在聊天的配对数: %d | 等待队列有无人: %t",
			totalUsers, len(partners)/2, len(waiting) > 0)
	}

	unpair := func(c *Client) {
		p := partners[c]
		if p == nil {
			return
		}

		// 计算配对双方的连线时长并输出日志
		if start, ok := pairStart[c]; ok {
			duration := time.Since(start).Round(time.Second)
			log.Printf("[配对断开] 用户(%s) 与 用户(%s) 结束了通话，本次联机时长: %v", c.ip, p.ip, duration)
			delete(pairStart, c)
			delete(pairStart, p)
		}

		delete(partners, c)
		delete(partners, p)
		p.sendCtrl(ctrlMsg{kind: ctrlPartnerLeft}) // 非阻塞
	}

	pair := func(c *Client, o *Client) {
		partners[c] = o
		partners[o] = c

		// 配对成功，记录当下的联机时间点
		now := time.Now()
		pairStart[c] = now
		pairStart[o] = now
		log.Printf("[配对成功] 用户(%s) ─── 用户(%s) 成功连线", c.ip, o.ip)

		c.sendCtrl(ctrlMsg{kind: ctrlMatched, partner: o})
		o.sendCtrl(ctrlMsg{kind: ctrlMatched, partner: c})
	}

	enqueue := func(c *Client) {
		for i, w := range waiting {
			if matches(c, w) {
				waiting = append(waiting[:i], waiting[i+1:]...)
				pair(c, w)
				return
			}
		}
		waiting = append(waiting, c)
		log.Printf("队列当前等待人数 %d", len(waiting))
	}

	for m := range matchbox {
		switch m.kind {
		case mmRegister:
			totalUsers++
			log.Printf("[用户进入] 收到来自 IP: %s 的新 WebSocket 连接", m.c.ip)
			enqueue(m.c)
			logStatus()

		case mmSkip:
			log.Printf("[用户指令] 用户(%s) 触发了 /skip 换人机制", m.c.ip)
			unpair(m.c)
			enqueue(m.c)
			logStatus()

		case mmGone:
			totalUsers--
			// 计算用户从连入服务到最终彻底断开的长短
			stayDuration := time.Since(m.c.connectTime).Round(time.Second)
			log.Printf("[用户离开] 用户(%s) 已断开，在树洞内总停留时长: %v", m.c.ip, stayDuration)

			for i, w := range waiting {
				if w == m.c {
					waiting = append(waiting[:i], waiting[i+1:]...)
					break
				}
			}
			unpair(m.c) // 若已配对则通知对方
			logStatus()
		}
	}
}
func matches(a, b *Client) bool {
	// 任一方没有 seeking 偏好 → 随便配，直接匹配
	if a.seeking == "" || b.seeking == "" {
		return true
	}
	// 双方都有 seeking 偏好，但任一方没有填性别 → 无法判断，不匹配
	if a.gender == "" || b.gender == "" {
		return false
	}
	sameGender := a.gender == b.gender
	// 双方的 seeking 必须互相满足
	aOK := (a.seeking == "opposite" && !sameGender) || (a.seeking == "same" && sameGender)
	bOK := (b.seeking == "opposite" && !sameGender) || (b.seeking == "same" && sameGender)
	return aOK && bOK
}

// ───────────────────────── 限流(标准库令牌桶,按 IP)─────────────────────────

type bucket struct {
	tokens float64
	last   time.Time
}

type limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64 // 每秒补充
	burst   float64 // 桶容量
}

func newLimiter(rate, burst float64) *limiter {
	return &limiter{buckets: make(map[string]*bucket), rate: rate, burst: burst}
}

func (l *limiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	b := l.buckets[key]
	if b == nil {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

func (l *limiter) cleanup() {
	l.mu.Lock()
	defer l.mu.Unlock()
	cut := time.Now().Add(-ipBucketTTL)
	for k, b := range l.buckets {
		if b.last.Before(cut) {
			delete(l.buckets, k)
		}
	}
}

var (
	matchLimiter = newLimiter(matchRatePerSec, matchBurst)
	msgLimiter   = newLimiter(msgRatePerSec, msgBurst)
)

func cleanupBuckets() {
	t := time.NewTicker(ipBucketTTL)
	defer t.Stop()
	for range t.C {
		matchLimiter.cleanup()
		msgLimiter.cleanup()
	}
}

// ───────────────────────── 跨域 / IP ─────────────────────────

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     checkOrigin,
}

var allowedOrigins = parseAllowedOrigins(os.Getenv("ALLOWED_ORIGIN"))

func parseAllowedOrigins(raw string) []string {
	var out []string
	for _, s := range strings.Split(raw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // 非浏览器或同源
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	host := u.Hostname()
	if strings.EqualFold(u.Host, r.Host) || strings.EqualFold(host, hostnameOnly(r.Host)) {
		return true
	}
	if host == "fly.dev" || strings.HasSuffix(host, ".fly.dev") {
		return true
	}
	if host == "localhost" || host == "127.0.0.1" {
		return true
	}
	for _, a := range allowedOrigins {
		if strings.EqualFold(origin, a) {
			return true
		}
	}
	return false
}

func hostnameOnly(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

// clientIP 取限流键。上了反代会读 X-Forwarded-For / X-Real-IP 首段;否则用 RemoteAddr。
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i != -1 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	if xr := r.Header.Get("X-Real-IP"); xr != "" {
		return strings.TrimSpace(xr)
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// ───────────────────────── HTTP ─────────────────────────

func serveWs(w http.ResponseWriter, r *http.Request) {
	// ──────────────────────── 核心公网防线 ────────────────────────
	// 从 URL 的 Query 参数中提取我们约定的暗号 "secret"
	secret := r.URL.Query().Get("secret")
	gender := r.URL.Query().Get("gender")
	seeking := r.URL.Query().Get("seeking")
	wssecret := os.Getenv("WS_SECRET")
	if wssecret == "" {
		wssecret = "Mingda2026"
	}
	// 如果暗号不匹配，在门口直接熔断，返回 403 并不给升级 WebSocket 的机会
	if secret != wssecret {
		log.Printf("[拦截公网盲扫] 拒绝来自 IP: %s 的未授权握手探测", clientIP(r))
		http.Error(w, "Unauthorized Access", http.StatusForbidden)
		return
	}
	// ─────────────────────────────────────────────────────────────

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade:", err)
		return
	}
	c := &Client{
		conn:        conn,
		ip:          clientIP(r),
		gender:      gender,
		seeking:     seeking,
		ctrl:        make(chan ctrlMsg, ctrlBuf),
		in:          make(chan inMsg, inBuf),
		out:         make(chan []byte, outBuf),
		done:        make(chan struct{}),
		connectTime: time.Now(), // 初始化用户接入时间
	}
	go c.writePump()
	go c.loop()
	go c.readPump()
	matchbox <- mmMsg{kind: mmRegister, c: c} // 首次入队
}

func main() {
	go matchmaker()
	go cleanupBuckets()
	http.HandleFunc("/ws", serveWs)
	// 有 ./static 就用它;否则提供一个内置最小测试页,开箱即可双标签测试。
	if _, err := os.Stat("./static/index.html"); err == nil {
		http.Handle("/", http.FileServer(http.Dir("./static")))
	} else {
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
		})
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8888"
	}
	addr := ":" + port
	log.Println("夜话服务启动于 http://localhost" + addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
