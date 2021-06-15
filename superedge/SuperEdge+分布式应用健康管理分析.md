SuperEdge 在 2021-05-20 发布了 v0.3.0 版本，其中有一个重要功能更新叫**节点智能感知技术**。以下是对该功能的原文描述:

> 在原生 Kubernetes 设计中，所有节点可以从 Apiserver 更新 Endpoints数据，从而避免将流量路由到异常的节点上，以便提高服务 SLA。而在边缘计算的场景下，节点与 ApiServer 经常面临弱网断连的情况，处于断联期间的节点无法获取到更新的 Endpoints 数据，导致服务 SLA 大幅下降现象。
>
> 为了解决这个问题，SuperEdge 实现了节点智能感知技术，该技术基于 SuperEdge 首创的边缘分布式健康探测技术(EdgeHealth)和服务区域自治技术(ServiceGroup)，让处于断联状态的节点也可以感知并剔除异常 Endpoints，让边缘业务更可靠。
>
> 举例，集群中存在 A,B,C 三节点，一个服务 Svc 的后端实例均匀地分布在三个节点上，A 节点与云端断联后，B 节点故障，由于 A 节点上缓存的仍是断联前的 Service 的 Endpoints 列表，因此对于 Service 的访问仍旧会转发到 B 节点上，造成访问的失败；在使用 SuperEdge 的节点智能感知技术后，A 节点可以自行将属于 B 节点上的后端摘除，保证了服务访问的正常。
>
> 面对更为极端的情况，集群中所有节点同云端断联，节点也依然能保证服务后端的可用，极大增强了边缘集群的自治能力。

这个功能一定程度上解决了边缘计算场景下，云边网络不稳定或者断网的情况下，边缘端通过自治感知Service的Endpoint变化，是边缘端流量能够正确地流向健康的Endpoint。这个功能的实现是通过edge-health和application-grid-wrapper两个组件，本文也将通过源码分析这两个组件来了解该功能的具体实现。

# edge-health

edge-health 是作为一个daemon组件运行在边缘端的没一个节点上，通过这一个节点SuperEdge可以分布式的去判断节点的健康状态儿不仅仅由云端来判断。

首先先看看edge-health的相关数据结构：

``` go
type NodeListData struct {
	NodeList   v1.NodeList
	NodeListMu *sync.Mutex
}

type CheckInfoData struct {
	CheckInfo       map[string]map[string]float64 //string:checked ip string:Plugin name int:check score
	CheckInfoDataMu *sync.Mutex
}

type ResultDetail struct {
	Normal bool
	Hamc   string
	Time   time.Time
}

type ResultData struct {
	Result       map[string]map[string]ResultDetail //string:checker ip string:checked ip bool: whether normal
	ResultDataMu *sync.Mutex
}

type CommunicateData struct {
	SourceIP     string                  //clientIP，checker ip
	ResultDetail map[string]ResultDetail //checdedip checkdetail
	Hmac         string
}
```

含义如下：

* NodeListData：为了实现边缘分区域检查检测机制而维护的边缘节点cache，包含改区域内所有节点列表NodeList

* CheckInfoData：存放健康检查的信息数据，为一个二级map，第一级key为被检查节点的ip，第二级key是检测的插件的名称，value是检测分数

* ResultData：存放健康检查的结果数据，为一个二级map，第一级key为检查者ip，第二级key为被检查节点的ip，value为ResultDetail
  * ResultDetail：存放检查结果的详情信息
    * Time: 表示得出该结果时的时间，用于结果有效性的判断(超过一段时间没有更新的结果将无效)
    * Normal：Normal 为 true 表示检查结果正常；false 表示异常
    * Hmac：SourceIP 以及 CheckDetail 进行 hmac 得到，用于边缘节点通信过程中判断传输数据的有效性(是否被篡改)



edge-health的主体逻辑主要包含四个部分：

* GetNodeList：根据边缘节点的zone刷新节点的NodeList缓存，并更新NodeListData缓存数据

* Check：对每个需要进行健康检测的节点进行检测，现在支持两种插件检测（ping，kubelet），并将检测分数汇总并根据用户设置的基准线得出被检测的节点是否健康的结果。这个方法的实际执行者是各个插件的CheckExecute方法

* Commun：将本节点的检测结果发送给其他节点，并且接收其他节点发送过来的检测结果

* Vote：投票决定某个节点健康与否，如果某个节点被大多数(>1/2)节点判定为正常

以下依次对上述功能进行代码分析：

## GetNodeList

GetNodeList 每隔 HealthCheckPeriod 秒 (health-check-period 选项)执行一次，会按照如下情况分类刷新 node cache：

```go
go wait.Until(check.GetNodeList, time.Duration(check.GetHealthCheckPeriod())*time.Second, ctx.Done())
```

* 如果edge-system namespace下不存在edge-health-zone-config这个configmap，则没有开启多地域探测，因此会获取全部边缘节点并刷新node cache
* 否则，如果 edge-health-zone-config 的 configmap 数据部分 TaintZoneAdmission 为 false，则没有开启多地域探测，因此会获取所有边缘节点列表并刷新 node cache
* 如果 TaintZoneAdmission 为 true，且 node 有"superedgehealth/topology-zone"标签(标示区域)，则获取"superedgehealth/topology-zone" label value 相同的节点列表并刷新 node cache
* 如果 node 没有"superedgehealth/topology-zone" label，则只会将边缘节点本身添加到分布式健康检查节点列表中并刷新 node cache(only itself)

```go
func (c CheckEdge) GetNodeList() {
   var hostzone string
   var host *v1.Node

   masterSelector := labels.NewSelector()
   masterRequirement, err := labels.NewRequirement(common.MasterLabel, selection.DoesNotExist, []string{})
   if err != nil {
      klog.Errorf("can't new masterRequirement")
   }
   masterSelector = masterSelector.Add(*masterRequirement)

   if host, err = NodeManager.NodeLister.Get(common.HostName); err != nil {
      klog.Errorf("GetNodeList: can't get node with hostname %s, err: %v", common.HostName, err)
      return
   }

   if config, err := ConfigMapManager.ConfigMapLister.ConfigMaps(constant.NamespaceEdgeSystem).Get(common.TaintZoneConfig); err != nil { //multi-region cm not found
      if apierrors.IsNotFound(err) {
         if NodeList, err := NodeManager.NodeLister.List(masterSelector); err != nil {
            klog.Errorf("config not exist, get nodes err: %v", err)
            return
         } else {
            data.NodeList.SetNodeListDataByNodeSlice(NodeList)
         }
      } else {
         klog.Errorf("get ConfigMaps edge-health-zone-config err %v", err)
         return
      }
   } else { //multi-region cm found
      klog.V(4).Infof("cm value is %s", config.Data["TaintZoneAdmission"])
      if config.Data["TaintZoneAdmission"] == "false" { //close multi-region check
         if NodeList, err := NodeManager.NodeLister.List(masterSelector); err != nil {
            klog.Errorf("config exist, false, get nodes err : %v", err)
            return
         } else {
            data.NodeList.SetNodeListDataByNodeSlice(NodeList)
         }
      } else { //open multi-region check
         if _, ok := host.Labels[common.TopologyZone]; ok {
            hostzone = host.Labels[common.TopologyZone]
            klog.V(4).Infof("hostzone is %s", hostzone)

            masterzoneSelector := labels.NewSelector()
            zoneRequirement, err := labels.NewRequirement(common.TopologyZone, selection.Equals, []string{hostzone})
            if err != nil {
               klog.Errorf("can't new zoneRequirement")
            }
            masterzoneSelector = masterzoneSelector.Add(*masterRequirement, *zoneRequirement)
            if NodeList, err := NodeManager.NodeLister.List(masterzoneSelector); err != nil {
               klog.Errorf("config exist, true, host has zone label, get nodes err: %v", err)
               return
            } else {
               data.NodeList.SetNodeListDataByNodeSlice(NodeList)
            }
            klog.V(4).Infof("nodelist len is %d", data.NodeList.GetLenListData())
         } else {
            data.NodeList.SetNodeListDataByNodeSlice([]*v1.Node{host})
         }
      }
   }

   iplist := make(map[string]bool)
   tempItems := data.NodeList.CopyNodeListData()
   for _, v := range tempItems {
      for _, i := range v.Status.Addresses {
         if i.Type == v1.NodeInternalIP {
            iplist[i.Address] = true
            data.CheckInfoResult.SetCheckedIpCheckInfo(i.Address)
         }
      }
   }

   for _, v := range data.CheckInfoResult.TraverseCheckedIpCheckInfo() {
      if _, ok := iplist[v]; !ok {
         data.CheckInfoResult.DeleteCheckedIpCheckInfo(v)
      }
   }

   for k := range data.Result.CopyResultDataAll() {
      if _, ok := iplist[k]; !ok {
         data.Result.DeleteResultData(k)
      }
   }

   klog.V(4).Infof("GetNodeList: checkinfo is %v", data.CheckInfoResult)
}
```

在按照上述逻辑执行完以后，会初始化CheckInfoResult，设置需要被检查的节点的ip。还会删除不该本节点检测范围的CheckInfo和Result数据。

## Check

Check也是每隔HealthCheckPeriod 秒(health-check-period选项)执行一次

```go
go wait.Until(check.Check, time.Duration(check.GetHealthCheckPeriod())*time.Second, ctx.Done())
```

会对每个边缘节点执行若干种类的健康检查插件(ping，kubelet等)，并将各插件检查分数汇总，根据用户设置的基准线 HealthCheckScoreLine (health-check-scoreline 选项)得出节点是否健康的结果。

每种检查插件会有一个 Weight 参数，表示了该检查插件分数的权重值，所有权重参数之和应该为1，对应基准分数线 HealthCheckScoreLine  范围0-100。因此这里在设置分数时，会乘以权重。

回到 Check 函数，在调用各插件执行健康检查得出权重分数(CheckPluginScoreInfo)后，还需要将该分数与基准线 HealthCheckScoreLine 对比：如果高于(>=)分数线，则认为该节点本次检查正常；否则异常。

```go
func (c CheckEdge) Check() {
	wg := sync.WaitGroup{}
	wg.Add(c.CheckPluginsLen())
	for _, plugin := range c.GetCheckPlugins() {
		go plugin.CheckExecute(&wg)
	}
	wg.Wait()
	klog.V(4).Info("check finished")
	klog.V(4).Infof("healthcheck: after health check, checkinfo is %v", data.CheckInfoResult.CheckInfo)

	calculatetemp := data.CheckInfoResult.CopyCheckInfo()
	for desip, plugins := range calculatetemp {
		totalscore := 0.0
		for _, score := range plugins {
			totalscore += score
		}
		if totalscore >= c.HealthCheckScoreLine {
			data.Result.SetResultFromCheckInfo(common.LocalIp, desip, data.ResultDetail{Normal: true})
		} else {
			data.Result.SetResultFromCheckInfo(common.LocalIp, desip, data.ResultDetail{Normal: false})
		}
	}
	klog.V(4).Infof("healthcheck: after health check, result is %v", data.Result.Result)
}
```

每种插件具体执行健康检查的逻辑封装在 CheckExecute 中，这里以 ping plugin 为例：

```go
func (plugin PingCheckPlugin) CheckExecute(wg *sync.WaitGroup) {
	var err error
	execwg := sync.WaitGroup{}
	execwg.Add(len(data.CheckInfoResult.CheckInfo))
	for k := range data.CheckInfoResult.CopyCheckInfo() {
		temp := k
		go func(execwg *sync.WaitGroup) {
			for i := 0; i < plugin.HealthCheckRetryTime; i++ {
				if _, err = net.DialTimeout("tcp", temp+":"+strconv.Itoa(plugin.Port), time.Duration(plugin.HealthCheckoutTimeOut)*time.Second); err == nil {
					break
				}
			}
			if err == nil {
				klog.V(4).Infof("%s use %s plugin check %s successd", common.LocalIp, plugin.Name(), temp)
				data.CheckInfoResult.SetCheckInfo(temp, plugin.Name(), plugin.GetWeight(), 100)
			} else {
				klog.V(2).Infof("%s use %s plugin check %s failed, reason: %s", common.LocalIp, plugin.Name(), temp, err.Error())
				data.CheckInfoResult.SetCheckInfo(temp, plugin.Name(), plugin.GetWeight(), 0)
			}
			execwg.Done()
		}(&execwg)
	}
	execwg.Wait()
	wg.Done()
}
```

CheckExecute 会对同区域每个节点执行 ping 探测(net.DialTimeout)，如果失败，则给该节点打 CheckScoreMin  分(0)；否则，打 CheckScoreMax  分(100)。

每种检查插件会有一个 Weight 参数，表示了该检查插件分数的权重值，所有权重参数之和应该为1，对应基准分数线 HealthCheckScoreLine  范围0-100。因此这里在设置分数时，会乘以权重。

回到 Check 函数，在调用各插件执行健康检查得出权重分数(CheckPluginScoreInfo)后，还需要将该分数与基准线 HealthCheckScoreLine 对比：如果高于(>=)分数线，则认为该节点本次检查正常；否则异常。

```go
for desip, plugins := range calculatetemp {
		totalscore := 0.0
		for _, score := range plugins {
			totalscore += score
		}
		if totalscore >= c.HealthCheckScoreLine {
			data.Result.SetResultFromCheckInfo(common.LocalIp, desip, data.ResultDetail{Normal: true})
		} else {
			data.Result.SetResultFromCheckInfo(common.LocalIp, desip, data.ResultDetail{Normal: false})
		}
	}
	klog.V(4).Infof("healthcheck: after health check, result is %v", data.Result.Result)
}
```

## Commun

在对同区域各边缘节点执行健康检查后，需要将检查的结果传递给其它各节点，也需要接收别的节点的检测结果，这也就是 commun 模块负责的事情。

既然有发送也有接收所以就是Server和Client:

```go
go commun.Server(ctx, &wg)
go wait.Until(commun.Client, time.Duration(commun.GetPeriod())*time.Second, ctx.Done())
```

server负责接收其它边缘节点的检查结果，如下：

```go
func (c CommunicateEdge) Server(ctx context.Context, wg *sync.WaitGroup) {
   srv := &http.Server{Addr: ":" + strconv.Itoa(c.CommunicateServerPort)}
   srv.ReadTimeout = time.Duration(c.CommunicateTimeout) * time.Second
   srv.WriteTimeout = time.Duration(c.CommunicateTimeout) * time.Second
   http.HandleFunc("/result", func(w http.ResponseWriter, r *http.Request) {
      var communicatedata data.CommunicateData
      if r.Body == nil {
         http.Error(w, "Please send a request body", 401)
         return
      }

      err := json.NewDecoder(r.Body).Decode(&communicatedata)
      if err != nil {
         http.Error(w, err.Error(), 402)
         return
      }

      log.V(4).Infof("Communicate Server: received data from %v : %v", communicatedata.SourceIP, communicatedata.ResultDetail)
      if _, err := io.WriteString(w, "Received!\n"); err != nil {
         log.Errorf("Communicate Server: send response err : %v", err)
      }
      if hmac, err := util.GenerateHmac(communicatedata); err != nil {
         log.Errorf("Communicate Server: server GenerateHmac err: %v", err)
         return
      } else {
         if hmac != communicatedata.Hmac {
            log.Errorf("Communicate Server: Hmac not equal, hmac is %s, communicatedata.Hmac is %s", hmac, communicatedata.Hmac)
            http.Error(w, "Hmac not match", 403)
            return
         }
      }
      log.V(4).Infof("Communicate Server: Hmac match")

      data.Result.SetResult(&communicatedata)
      log.V(4).Infof("After communicate, result is %v", data.Result.Result)
   })

   http.HandleFunc("/debug/flags/v", pkgutil.UpdateLogLevel)

   http.HandleFunc("/localinfo", func(w http.ResponseWriter, r *http.Request) {
      localInfoData := data.Result.CopyLocalResultData(common.LocalIp)
      NodeInfo := make(map[string]data.ResultDetail)
      for ip, detail := range localInfoData {
         NodeInfo[util.GetNodeNameByIp(data.NodeList.NodeList.Items, ip)] = detail
      }
      if err := json.NewEncoder(w).Encode(NodeInfo); err != nil {
         log.Errorf("Get Local Info: NodeInfo err: %v", err)
         return
      }
   })

   go func() {
      if err := srv.ListenAndServe(); err != http.ErrServerClosed {
         log.Fatalf("Server: exit with error: %v", err)
      }
   }()

   for range ctx.Done() {
      ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
      cancel()
      if err := srv.Shutdown(ctx); err != nil {
         log.Errorf("Server: program exit, server exit")
      }
      wg.Done()
   }
}
```

负责接受其它边缘节点的检查结果，并写入自身检查结果 Result，流程如下：

* 通过`/result`路由接受请求，并将请求内容解析成 CommunicateData

* 对 CommunicateData 执行 GenerateHmac 获取hmac值，并与 CommunicateData 字段进行对比，检查接受数据的有效性

* 最后将 CommunicateData 检查结果写入 Result

  

Client  负责向其它节点发送本节点对它们的检查结果，如下：

```go
func (c CommunicateEdge) Client() {
   if _, ok := data.Result.Result[common.LocalIp]; ok {
      tempCommunicateData := data.Result.CopyLocalResultData(common.LocalIp)
      wg := sync.WaitGroup{}
      wg.Add(len(tempCommunicateData))
      for k := range tempCommunicateData { //send to
         des := k
         go func(wg *sync.WaitGroup) {
            if des != common.LocalIp {
               for i := 0; i < c.CommunicateRetryTime; i++ {
                  u := data.CommunicateData{SourceIP: common.LocalIp, ResultDetail: tempCommunicateData}
                  if hmac, err := util.GenerateHmac(u); err != nil {
                     log.Errorf("Communicate Client: generateHmac err: %v", err)
                  } else {
                     u.Hmac = hmac
                  }
                  log.V(4).Infof("Communicate Client: ready to put data: %v to %s", u, des)
                  requestByte, _ := json.Marshal(u)
                  requestReader := bytes.NewReader(requestByte)
                  ok := func() bool {
                     client := http.Client{Timeout: time.Duration(c.CommunicateTimeout) * time.Second}
                     req, err := http.NewRequest("PUT", "http://"+des+":"+strconv.Itoa(c.CommunicateServerPort)+"/result", requestReader)
                     if err != nil {
                        log.Errorf("Communicate Client: %s, NewRequest err: %s", des, err.Error())
                        return false
                     }

                     res, err := client.Do(req)
                     if err != nil {
                        log.Errorf("Communicate Client: communicate to %v failed %v", des, err)
                        return false
                     }
                     defer func() {
                        if res != nil {
                           res.Body.Close()
                        }
                     }()
                     if _, err := io.Copy(ioutil.Discard, res.Body); err != nil {
                        log.Errorf("io copy err: %s", err.Error())
                     }
                     if res.StatusCode != http.StatusOK {
                        log.Errorf("Communicate Client: httpResponse.StatusCode!=200, is %d", res.StatusCode)
                        return false
                     }

                     log.V(4).Infof("Communicate Client: put to %v status: %v succeed", des, u)
                     return true
                  }()
                  if ok {
                     break
                  }
               }
            }
            wg.Done()
         }(&wg)
      }
      wg.Wait()
   }
}
```

逻辑如下：

* 获取本地CommunicateData数据
* 尝试向每隔目标地址发送请求，尝试CommunicateRetryTime次，请求步骤：
  * 构建CommunicateData结构体，包括：
    * SourceIP：表示执行检查的ip
    * CheckDetail：为 Checked ip:Check detail 组织形式，包含被检查的ip以及检查结果
  * 调用 GenerateHmac 构建 Hmac：实际上是以 edge-system 下的 hmac-config configmap hmackey 字段为 key，对 SourceIP 以及 CheckDetail进行 hmac 得到，用于判断传输数据的有效性(是否被篡改)
  * 发送上述构建的 CommunicateData 给其它边缘节点 

## Vote

在接受到其它节点的健康检查结果后，vote 模块会每隔VotePeriod秒对结果进行统计得出最终判决，并向 apiserver 报告：

```go
vote := vote.NewVoteEdge(d.VoteTimeOut, d.VotePeriod)
go wait.Until(vote.Vote, time.Duration(vote.GetVotePeriod())*time.Second, ctx.Done())
```

如下：

```go
func (vote VoteEdge) Vote() {
   voteCountMap := make(map[string]map[string]int) // {"a":{"yes":1,"no":2}}
   healthNodeMap := make(map[string]string)

   tempNodeStatus := data.Result.CopyResultDataAll() //map[string]map[string]ResultDetail string:checker ip string:checked ip bool:noraml
   for k, v := range tempNodeStatus {                //k is checker ip
      for ip, resultdetail := range v { //ip is checked ip
         if k == common.LocalIp || (k != common.LocalIp && !time.Now().After(resultdetail.Time.Add(time.Duration(vote.GetVoteTimeout())*time.Second))) {
            healthNodeMap[k] = "" //node is a health node if it has at least one valid check
            if _, ok := voteCountMap[ip]; !ok {
               voteCountMap[ip] = make(map[string]int)
            }
            if resultdetail.Normal {
               if _, ok := voteCountMap[ip]["yes"]; !ok {
                  voteCountMap[ip]["yes"] = 0
               }
               voteCountMap[ip]["yes"] += 1
            } else {
               if _, ok := voteCountMap[ip]["no"]; !ok {
                  voteCountMap[ip]["no"] = 0
               }
               voteCountMap[ip]["no"] += 1
            }
         }
      }
   }
   log.V(4).Infof("Vote: healthNodeMap is %v , voteCountMap is %v", healthNodeMap, voteCountMap)

   //num := (float64(len(healthNodeMap)) + 1) / 2
   num := (float64(data.CheckInfoResult.GetLenCheckInfo()) + 1) / 2

   if len(healthNodeMap) == 1 {
      return
   }
   for ip, v := range voteCountMap {
      if _, ok := v["yes"]; ok {
         if float64(v["yes"]) >= num {
            log.V(4).Infof("vote: vote yes to master begin")
            name := util.GetNodeNameByIp(data.NodeList.NodeList.Items, ip)
            if node, err := check.NodeManager.NodeLister.Get(name); err == nil && name != "" {
               if _, ok := node.Annotations["nodeunhealth"]; ok {
                  nodenew := node.DeepCopy()
                  delete(nodenew.Annotations, "nodeunhealth")
                  if _, err := common.ClientSet.CoreV1().Nodes().Update(context.TODO(), nodenew, metav1.UpdateOptions{}); err != nil {
                     log.Errorf("update yes vote to master error: %v ", err)
                  } else {
                     log.V(2).Infof("update yes vote of %s to master", nodenew.Name)
                  }
               } else if index, flag := admissionutil.TaintExistsPosition(node.Spec.Taints, UnreachNoExecuteTaint); flag {
                  nodenew := node.DeepCopy()
                  nodenew.Spec.Taints = append(nodenew.Spec.Taints[:index], nodenew.Spec.Taints[index+1:]...)
                  if _, err := common.ClientSet.CoreV1().Nodes().Update(context.TODO(), nodenew, metav1.UpdateOptions{}); err != nil {
                     log.Errorf("remove no excute taint for health node error: %v ", err)
                  } else {
                     log.V(2).Infof("remove no excute taint for health node: %s to master", nodenew.Name)
                  }
               }
            }
         }
      }
      if _, ok := v["no"]; ok {
         if float64(v["no"]) >= num {
            log.V(4).Infof("vote: vote no to master begin")
            name := util.GetNodeNameByIp(data.NodeList.NodeList.Items, ip)
            if node, err := check.NodeManager.NodeLister.Get(name); err == nil && name != "" {
               if _, ok := node.Annotations["nodeunhealth"]; !ok {
                  nodenew := node.DeepCopy()
                  nodenew.Annotations["nodeunhealth"] = "yes"
                  if _, err := common.ClientSet.CoreV1().Nodes().Update(context.TODO(), nodenew, metav1.UpdateOptions{}); err != nil {
                     log.Errorf("update no vote to master error: %v ", err)
                  } else {
                     log.V(2).Infof("update no vote of %s to master", nodenew.Name)
                  }
               }
            }
         }
      }
   }
}
```

逻辑如下：

* 统计Result中被检测节点的Normal状态，正常yes加一，不正常no加一
* 统计状态为健康的节点的个数
* 如果健康节点的个数为1则直接放回，因为没必要自己跟自己投票
* 判断没一个被检测节点yes和no的个数：
  * 如果yes的个数大于等于`num := (float64(data.CheckInfoResult.GetLenCheckInfo()) + 1) / 2`，则说明该节点为健康节点，删除该节点的`nodeunhealth` annotation，并删除该节点上的`NoExecute`的taint
  * 如果no的个数大于等于`num := (float64(data.CheckInfoResult.GetLenCheckInfo()) + 1) / 2`，则说明该节点为不健康节点，给该节点添加的`nodeunhealth` annotation



到这儿edge-health模块就分析完了，下面我们分析application-grid-wrapper组件



# application-grid-wrapper

application-grid-wrapper是夹在liteapiserver和各个组件例如kube-proxy中间的组件，它相当于一个请求的拦截器，对kube-proxy所需要的数据进行校验，从而达到把不健康的应用的流量线路从iptables中剔除。下面是该组件的主体结构：

```go
type interceptorServer struct {
   restConfig                        *rest.Config
   cache                             storage.Cache
   serviceWatchCh                    <-chan watch.Event
   endpointsWatchCh                  <-chan watch.Event
   mediaSerializer                   []runtime.SerializerInfo
   serviceAutonomyEnhancementAddress string
}
```

该组件的启动入口：

```go
func (s *interceptorServer) Run(debug bool, bindAddress string, insecure bool, caFile, certFile, keyFile string, serviceAutonomyEnhancement options.ServiceAutonomyEnhancementOptions) error {
   ctx, cancel := context.WithCancel(context.Background())
   defer cancel()

   /* cache
    */
   if err := s.setupInformers(ctx.Done()); err != nil {
      return err
   }

   klog.Infof("Start to run GetLocalInfo client")

   if serviceAutonomyEnhancement.Enabled {
      go wait.Until(s.NodeStatusAcquisition, time.Duration(serviceAutonomyEnhancement.UpdateInterval)*time.Second, ctx.Done())
   }

   klog.Infof("Start to run interceptor server")
   /* filter
    */
   server := &http.Server{Addr: bindAddress, Handler: s.buildFilterChains(debug)}

   if insecure {
      return server.ListenAndServe()
   }

   tlsConfig := &tls.Config{}
   pool := x509.NewCertPool()
   caCrt, err := ioutil.ReadFile(caFile)
   if err != nil {
      klog.Errorf("can't read ca file %s, %v", caFile, err)
      return nil
   }
   pool.AppendCertsFromPEM(caCrt)
   tlsConfig.RootCAs = pool

   if len(certFile) != 0 && len(keyFile) != 0 {
      cert, err := tls.LoadX509KeyPair(certFile, keyFile)
      if err != nil {
         klog.Errorf("can't load certificate pair %s %s, %v", certFile, keyFile, err)
         return nil
      }
      tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
   }

   server.TLSConfig = tlsConfig
   return server.ListenAndServeTLS("", "")
}
```

逻辑如下：

* 启动Informer，总共有三个Informer被启动，分别是NodeInformer，ServiceInformer，EndpointInformer
* 如果开启的服务自治增加功能`serviceAutonomyEnhancement`，那么每隔`UpdateInterval`秒获取本地node健康检查的结果

server启动以后注册相关handler：

```go
func (s *interceptorServer) buildFilterChains(debug bool) http.Handler {
   handler := http.Handler(http.NewServeMux())

   handler = s.interceptEndpointsRequest(handler)
   handler = s.interceptServiceRequest(handler)
   handler = s.interceptEventRequest(handler)
   handler = s.interceptNodeRequest(handler)
   handler = s.logger(handler)

   if debug {
      handler = s.debugger(handler)
   }

   return handler
}
```

主要是接受关于Endpoint、Service、Event和Node的请求

再来看看本地cache的数据结构：

```go
type storageCache struct {
   // hostName is the nodeName of node which application-grid-wrapper deploys on
   hostName                          string
   wrapperInCluster                  bool
   serviceAutonomyEnhancementEnabled bool

   // mu lock protect the following map structure
   mu            sync.RWMutex
   servicesMap   map[types.NamespacedName]*serviceContainer
   endpointsMap  map[types.NamespacedName]*endpointsContainer
   nodesMap      map[types.NamespacedName]*nodeContainer
   localNodeInfo map[string]data.ResultDetail

   // service watch channel
   serviceChan chan<- watch.Event
   // endpoints watch channel
   endpointsChan chan<- watch.Event
}
```

其中缓存了本地node的健康信息，还有service和endpoint等相关缓存。通过informer注册的handler拿到云端的数据以后通过本地

localNodeInfo数据对数据进行校验，把不健康的node上的应用的从endpoint中剔除，下面以更新endpoint的handler为例：

```go
func (eh *endpointsHandler) update(endpoints *v1.Endpoints) {
   sc := eh.cache

   sc.mu.Lock()
   endpointsKey := types.NamespacedName{Namespace: endpoints.Namespace, Name: endpoints.Name}
   klog.Infof("Updating endpoints %v", endpointsKey)

   endpointsContainer, found := sc.endpointsMap[endpointsKey]
   if !found {
      sc.mu.Unlock()
      klog.Errorf("Updating non-existed endpoints %v", endpointsKey)
      return
   }
   endpointsContainer.endpoints = endpoints
   newEps := pruneEndpoints(sc.hostName, sc.nodesMap, sc.servicesMap, endpoints, sc.localNodeInfo, sc.wrapperInCluster, sc.serviceAutonomyEnhancementEnabled)
   changed := !apiequality.Semantic.DeepEqual(endpointsContainer.modified, newEps)
   if changed {
      endpointsContainer.modified = newEps
   }
   sc.mu.Unlock()

   if changed {
      sc.endpointsChan <- watch.Event{
         Type:   watch.Modified,
         Object: newEps,
      }
   }
}
```

拿到云端的数据以后通过pruneEndpoints方法对数据进行检验：

```go
func pruneEndpoints(hostName string,
   nodes map[types.NamespacedName]*nodeContainer,
   services map[types.NamespacedName]*serviceContainer,
   eps *v1.Endpoints, localNodeInfo map[string]data.ResultDetail, wrapperInCluster, serviceAutonomyEnhancementEnabled bool) *v1.Endpoints {

   epsKey := types.NamespacedName{Namespace: eps.Namespace, Name: eps.Name}

   if wrapperInCluster {
      eps = genLocalEndpoints(eps)
   }

   // dangling endpoints
   svc, ok := services[epsKey]
   if !ok {
      klog.V(4).Infof("Dangling endpoints %s, %+#v", eps.Name, eps.Subsets)
      return eps
   }

   // normal service
   if len(svc.keys) == 0 {
      klog.V(4).Infof("Normal endpoints %s, %+#v", eps.Name, eps.Subsets)
      if eps.Namespace == metav1.NamespaceDefault && eps.Name == MasterEndpointName {
         return eps
      }
      if serviceAutonomyEnhancementEnabled {
         newEps := eps.DeepCopy()
         for si := range newEps.Subsets {
            subnet := &newEps.Subsets[si]
            subnet.Addresses = filterLocalNodeInfoConcernedAddresses(nodes, subnet.Addresses, localNodeInfo)
            subnet.NotReadyAddresses = filterLocalNodeInfoConcernedAddresses(nodes, subnet.NotReadyAddresses, localNodeInfo)
         }
         klog.V(4).Infof("Normal endpoints after LocalNodeInfo filter %s: subnets from %+#v to %+#v", eps.Name, eps.Subsets, newEps.Subsets)
         return newEps
      }
      return eps
   }

   // topology endpoints
   newEps := eps.DeepCopy()
   for si := range newEps.Subsets {
      subnet := &newEps.Subsets[si]
      subnet.Addresses = filterConcernedAddresses(svc.keys, hostName, nodes, subnet.Addresses, localNodeInfo, serviceAutonomyEnhancementEnabled)
      subnet.NotReadyAddresses = filterConcernedAddresses(svc.keys, hostName, nodes, subnet.NotReadyAddresses, localNodeInfo, serviceAutonomyEnhancementEnabled)
   }
   klog.V(4).Infof("Topology endpoints %s: subnets from %+#v to %+#v", eps.Name, eps.Subsets, newEps.Subsets)

   return newEps
}
```

方法中可以看到，如果serviceAutonomyEnhancementEnabled开启了通过filterLocalNodeInfoConcernedAddresses方法就可以对本地已经缓存下来的数据进行筛选

```go
func filterLocalNodeInfoConcernedAddresses(nodes map[types.NamespacedName]*nodeContainer,
   addresses []v1.EndpointAddress, localNodeInfo map[string]data.ResultDetail) []v1.EndpointAddress {

   filteredEndpointAddresses := make([]v1.EndpointAddress, 0)
   for i := range addresses {
      addr := addresses[i]
      if nodeName := addr.NodeName; nodeName != nil {
         epsNode, found := nodes[types.NamespacedName{Name: *nodeName}]
         if !found {
            continue
         }
         _, found = localNodeInfo[epsNode.node.Name]
         if !found || (found && localNodeInfo[epsNode.node.Name].Normal) {
            filteredEndpointAddresses = append(filteredEndpointAddresses, addr)
         }
      }
   }

   return filteredEndpointAddresses
}
```

从方法中可以看到如果该EndpointAddress所在的node在localNodeInfo中找不到该节点或者节点被标记为不健康那么这个EndpointAddress将被筛选出去，从而kube-proxy拿到的数据就是正确的数据。

# 总结

本文通过代码介绍的edge-health和application-grid-wapper两个组件的原理，通过edge-health得到本地node的检测检查的数据，然后application-grid-wrapper该数据对kube-proxy所需要的数据进行校验从而写入正确的流量路径，或者说把不健康的endpoint剔除，通过这两个组件从而实现了分布式应用健康管理。