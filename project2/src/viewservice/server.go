package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	pingTracker	map[string]time.Time
	currView 	View
	acked		bool
	idle		[]string
}

func idleContains(idle []string, target string) bool {
	for  _, server := range idle {
		if server == target {
			return true
		}
	}
	return false
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	fmt.Println("vs primary status: ", vs.currView.Primary)
	fmt.Println("vs backup status: ", vs.currView.Backup)
	fmt.Println("args me status: ", args.Me)
	if vs.currView.Viewnum == 0 {
		if args.Viewnum == 0 {
			vs.currView.Primary = args.Me
			vs.acked = false
			vs.currView.Viewnum++
		}		
	}else{
		if vs.acked == false {
			if args.Me == vs.currView.Primary{
				if args.Viewnum == vs.currView.Viewnum {
					vs.acked = true
					if vs.currView.Backup == "" && len(vs.idle) > 0 {
						vs.currView.Backup = vs.idle[0]
						vs.idle = vs.idle[1:]
					}
				}
			}else if args.Me == vs.currView.Backup {
				//do nothing
			}else {
				//other pings
				if !idleContains(vs.idle, args.Me){
					vs.idle = append(vs.idle, args.Me)
				}
			}
		}else {
			if args.Me == vs.currView.Primary {
				//do nothing if primary's ping number is greater than 0 and less than current view
				//do nothing if primary is current view
				//if primay's viewnum == 0 => primary restarts: ck1 becomes backup
				if args.Viewnum == 0 {
					if vs.currView.Backup != ""{
						vs.currView.Primary = vs.currView.Backup
						vs.currView.Backup = args.Me
						vs.acked = false
						vs.currView.Viewnum++
					}
				}
			}else if args.Me == vs.currView.Backup {
				//dead backup pings???
				if args.Viewnum == 0 {
					 vs.currView.Backup = ""
					 if !idleContains(vs.idle, args.Me) {
					 	vs.idle = append(vs.idle, args.Me)
					 }
					 vs.acked = false
					 vs.currView.Viewnum++
				}
			}else {
				//other pings
				if vs.currView.Backup == ""{
					if !idleContains(vs.idle, args.Me){
						vs.currView.Backup = args.Me
					}else {
						vs.currView.Backup = vs.idle[0]
						vs.idle = vs.idle[1:]
					}
					vs.currView.Viewnum++
					vs.acked = false
				} else {
					if !idleContains(vs.idle, args.Me) {
						vs.idle = append(vs.idle, args.Me)
					}
				}
			}
		}
	}

	vs.pingTracker[args.Me] = time.Now()
	reply.View = vs.currView
	fmt.Println("Print reply view",reply.View.Primary, "; ", reply.View.Backup)
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.currView
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	for k, v := range vs.pingTracker {
		server := k
		difference := time.Since(v)
		if difference > PingInterval * DeadPings {
			switch server {
			case vs.currView.Primary:
				if vs.acked {
					if vs.currView.Backup != "" {
						vs.currView.Primary = vs.currView.Backup
						vs.currView.Backup = ""
						if len(vs.idle) > 0 {
							vs.currView.Backup = vs.idle[0]
							vs.idle = vs.idle[1:]
						}
						vs.acked = false
						vs.pingTracker[k] = time.Now()
						vs.currView.Viewnum++
					}
				}else {
					fmt.Println("Crash")
				}
			case vs.currView.Backup:
				if vs.acked {
					vs.currView.Backup = ""
					fmt.Println("backup is set to nil")
					if len(vs.idle) > 0 {
						vs.currView.Backup = vs.idle[0]
						vs.idle = vs.idle[1:]
					}
					vs.acked = false
					vs.currView.Viewnum++
				}else {
					fmt.Println("Crash")
				}
			default:
				for i, idleServer := range vs.idle {
					if server == idleServer {
						vs.idle = append(vs.idle[0:i], vs.idle[i+1:]...)
					}
				}
			}
		}
	}

}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.idle = make([]string, 0)
	vs.currView = View{0, "", ""}
	vs.acked = false
	vs.pingTracker = make(map[string]time.Time)
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		//Package atomic provides low-level atomic memory primitives useful for 
		//implementing synchronization algorithms.
		//为了保证并发安全，除了使用临界区之外，还可以使用原子操作。顾名思义这类操作满足原子性，
		//其执行过程不能被中断，这也就保证了同一时刻一个线程的执行不会被其他线程中断，也保证了多线程下数据操作的一致性
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
