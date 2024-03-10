# My-Raft base on 6.824

### 项目介绍

基于MIT课程<a href="https://pdos.csail.mit.edu/6.824/labs/lab-raft.html">6.824 Lab3</a>与论文<a href="https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf">In Search of an Understandable Consensus Algorithm (Extended Version)</a>实现的Golang版本的分布式共识协议Raft，其中rpc框架和测试脚本来自课程6.824（现为6.5840）

通过了所有A、B、C、D测试，根据论文完整实现了Raft分布式共识协议，包含领导选举、日志同步、持久化、Snapshot等部分，确保了非拜占庭条件下的安全性，包括网络延迟、分区、数据包丢失、重复和乱序等，可通过`go test`验收



> ⚠️：跟据论文，在新的leader胜任时应广播一次command为nil的log，以追commit前任的log，但测试脚本部分环节要求index和Start()传入的顺序一致，故注释掉releaseBlankEntry相关的部分才可以通过测试3A、B、C



附：<a href="https://github.com/Breeze-P/my-raft/DEVLOG.md">开发文档</a>