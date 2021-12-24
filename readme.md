## Lab 1 -  Report

- Date:2020-5-28

  

> 实验报告中，包含以下方面的内容：
>
> - 对代码框架的分析；
> - 实验要求完成的内容，你的解决思路；
> - 解析你的核心代码。



## Analysis of the given code framework

由于*Sequential*的实现比较简单，这部分主要通过分析*Distributed*分析得到代码框架

- 用户程序调用位于**master.go**中的`distributed`函数开启mapreduce对输入文件的处理

- `distributed`函数初始化一个`master`对象，通过`startRPCServer`开启master的rpc server以接受worker的rpc，并调用master的run函数，传入`schedule`和`finish`函数
- master的`run`函数分别对`mapPhase`和`reducePhase`调用`schedule`，这个`schedule`函数通过`forwardRegistrations`将新注册的worker加入到`registerChan`中，与`schedule`函数(in **schedule.go**)共享当前注册的worker的地址信息
- `schedule`负责调度worker执行所有的task，这里需要通过goroutine实现并发控制以及通过channel实现goroutine和主进程的通信，调度worker通过rpc的调用
- worker中的`doTask`函数可以根据分配到任务的类型调用对应的`doMap`和`doReduce`函数对文件和中间文件进行处理，生成指定的目标文件
- `schedule`返回之后`run`调用`finish`函数关掉rpc server和所有的worker，最后调用`merge`合并reduce所有的输出文件

## Part I: Map/Reduce input and output

- 实验要求：完成doMap函数，分割Map Task的输出
- 解决思路：
  - 首先调用mapF函数将输入的文件映射到key/value pairs
  - 通过对key进行hash运算之后取模获得对应的文件编号，按照文件编号分组保存
  - 将key/value pairs转为json之后写入到对应的文件中

- 核心代码：**doMap**  in [common_ma.go](./src/mapreduce/common_map.go) 主要有以下几个注意点
  - 输入文件`inFile`需要先借助`ioutil`工具的read函数获得文件内容之后再输入到`mapF`中
  - 根据目标文件建立二维数组暂存`KeyValue` pairs 之后可以统一写入文件，避免多次open/close造成的性能损失
  - 注意`enc.Encode`的参数应该用值传递

```go
func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	//
	// doMap manages one map task: it should read one of the input files
	// (inFile), call the user-defined map function (mapF) for that file's
	// contents, and partition mapF's output into nReduce intermediate files.
  //
	// 1. Invoke the MapF to map the file to nReduce key/value pairs
	contents, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal("Failed to read file: " + inFile)
	}
	var kvs = mapF(inFile, string(contents))
  
  //2. reorganize the key/value pairs by reduce file number
	var tmp [][]KeyValue
	for i := 0; i < nReduce; i++ {
		tmp = append(tmp,[]KeyValue{})
	}
	for i := 0; i < len(kvs); i++ {
		var n = int(math.Mod(float64(ihash(kvs[i].Key)), float64(nReduce)))
		tmp[n] = append(tmp[n],kvs[i])
	}

	// 3. write the data to correspond files
	for i := 0; i < nReduce; i++ {
		if len(tmp[i])!=0 {
			var filename = reduceName(jobName, mapTask, i)
			f, error := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
			if error != nil {
				fmt.Println(error)
			}
			// 4. Turn the key/value pairs to json and write to a file
			enc := json.NewEncoder(f)
			for _, kv := range tmp[i] {
				error := enc.Encode(&kv)
				if error != nil {
					fmt.Println(error)
				}
			}
			f.Close()
		}
	}
}
```

- 实验要求：聚集一个Reduce task所有的输入
- 解决思路：
  - 读出该reduce task所有对应的中间文件的内容
  - 调用解码器对文件内容进行解码并统一放到key/value pair数组里面
  - 对key/value pair的数组进行排序
  - 对每个key构建value数组调用`reduceF`获得最终value值
  - 将最终的key/value pair编码后写入到文件中

- 核心代码：**doReduce**  in [common_reduce.go](./src/mapreduce/common_reduce.go) 主要有以下几个注意点
  - 排序可以参考 Library中的sort排序的[示例](sort/example_interface_test.go), 这里不必太过纠结key的大小比较是否按照数字本身的意义，只要统一排序方式就可以
  - 整理keyvalue pair获得所有key对应的values注意算法的调整，最后的key不要忘记记录

```go
// sort functions
type ByKey [] KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool {
	return  a[i].Key< a[j].Key
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
  //
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// 1. Read one intermediate file from each map task
	var kvs []KeyValue
	for i := 0; i < nMap; i++ {
		var filename = reduceName(jobName, i, reduceTask)
		rf,err1 := os.OpenFile(filename,os.O_RDWR,0755)
		if err1 != nil{
			fmt.Println(err1)
		}
		// 2. Decode the intermediate file into a key/value array
		enc := json.NewDecoder(rf)
		var err2 error = nil
		var kv KeyValue
		for err2 == nil {
			err2 = enc.Decode(&kv)
			kvs = append(kvs,kv)
		}
		rf.Close()
	}
	// 3. Sort the key/value array
	sort.Sort(ByKey(kvs))
	
	// 4. Call reduceF for each key
	var curKey string
	var curValues []string
	var newKeyValues [] KeyValue
	for _, kv := range kvs{
		if curKey != kv.Key {
			if len(curValues) != 0 {
				newKeyValues = append(newKeyValues, KeyValue{curKey,reduceF(curKey,curValues) })
			}
			curKey = kv.Key
			curValues = []string{}
			curValues = append(curValues, kv.Value)
		} else {
			curValues = append(curValues, kv.Value)
		}
	}
	if len(curValues) != 0 {
		newKeyValues = append(newKeyValues, KeyValue{curKey,reduceF(curKey,curValues) })
	}

	//5. write to the output file
	f, error := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE, 0644)
	if error != nil {
		fmt.Println(error)
	}
	enc := json.NewEncoder(f)
	for _,kv := range newKeyValues {
		error := enc.Encode(&kv)
		if error != nil {
			fmt.Println(error)
		}
	}
	f.Close()
}
```



## Part II: Single-worker word count

- 实验要求：通过mapF和reduceF函数实现文件中单词的计数
- 解决思路：
  - mapF函数中将文档的内容按单词进行解析，并且将单词作为key，对应的value为1
  - reduce对key所有的value求和之后返回
- 核心代码：**mapF** and **reduceF** in [wc.go](src/main/wc.go) 主要有以下几个注意点
  - 使用`strings.FieldsFunc`处理单词时对应的func函数返回值决定了跳过的字符，所以如果`unicode.IsLetter`为true时应该返回false
  - 使用`strconv.Itoa()`可以将字符串转为数字

```go
//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func mapF(filename string, contents string) (res []mapreduce.KeyValue) {
	words := strings.FieldsFunc(contents,func(c rune) bool {
		return !unicode.IsLetter(c)})
	for _, w := range words {
		kv := mapreduce.KeyValue{w, "1"}
		res = append(res, kv)
	}
	return
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func reduceF(key string, values []string) string {
	var result int = 0
	for _,v := range values {
		tmp,_ := strconv.Atoi(v)
		result = result + tmp
	}
	return strconv.Itoa(result)
}
```



## Part III: Distributing MapReduce tasks

- 实验要求：完成schedule函数，实现分布式系统下多个worker的调度，实现并发工作
- 解决思路：
  - 主进程为每一个task从registerChan中读取一个空闲的worker
  - 通过goroutine调用rpc实现worker的并发调度
  - 等到所有任务都完成之后退出
- 核心代码：**schedule** in [schedule.go](src/mapreduce/schedule.go) 主要有以下几个注意点
  - 当一个worker完成之后，可以通过将这个worker的addr再插入到`channel`中供主进程调度，无须通过循环遍历`channel`来调度
  - `sync.WaitGroup`和`channel`共同使用时注意调用的顺序，由于`channel`中放入对象但是没有人读取的话会造成程序的堵塞，所以完成rpc调用之后可以先调用`w.done()`这样最后一个task完成之后就可以直接唤醒`w.wait()`避免了最后一次插入`channel`造成的阻塞

```go
//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
   var ntasks int
   var n_other int // number of inputs (for reduce) or outputs (for map)
   switch phase {
   case mapPhase:
      ntasks = len(mapFiles)
      n_other = nReduce
   case reducePhase:
      ntasks = nReduce
      n_other = len(mapFiles)
   }

   fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

   // All ntasks tasks have to be scheduled on workers. Once all tasks
   // have completed successfully, schedule() should return.
   var wg sync.WaitGroup
   a := make(chan int, ntasks)
   for i:=0;i<ntasks;i++ {
      a <-i
   }
   wg.Add(ntasks)
   Loop: for {
      select {
         case i :=<-a:
         fmt.Printf("begin handle %d\n",i)
         //1. arrange a worker for each task if any idle worker exists
         addr :=<-registerChan
         //2. prepare DoTaskArgs
         var args DoTaskArgs
         switch phase {
         case mapPhase:
            args = DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
         case reducePhase:
            args = DoTaskArgs{jobName, "", phase, i, n_other}
         }
         //3. start goroutine to handle the tasks
         go func(i int) {
            var index = i
            ret := call(addr, "Worker.DoTask", args, nil)
            if ret == true {
               fmt.Printf("Success handle %d task\n", index)
               wg.Done()
               registerChan <- addr
            }else{
               fmt.Printf("Fail handle %d task\n", index)
               a <- index
            }
         }(i)
      default:
         fmt.Printf("There is no other tasks to be done\n")
         break Loop
      }
   }
   //3. waite for all the tasks to end
   wg.Wait()
   fmt.Printf("Schedule: %v done\n", phase)
}
```



## Part IV: Handling worker failures

- 实验要求：处理可能出现的worker failure的情况
- 解决思路：
  - 将当前未完成的任务放到channel中，当channel为空时循环结束

- 核心代码：代码如PartIII，主要有以下几个注意点
  - 通过channel来控制task的处理并判断是否结束无法可以借助于for和select语句，读到空时，通过break for循环的标签跳出循环
  - goroutine和主进程的通信通过channel进行比较合理，借助全局变量可能会造成数据的race，也不能有效实现期待的代码效果



## Part V: Inverted index generation (OPTIONAL)

- 实验要求：通过**mapF** and **reduce**实现inverted index
- 解决思路：
  - mapF中将document中的word作为key，document作为value生成key/value pair
  - reduceF对每一个key的value数组去重处理之后合并返回该key的value

- 核心代码：**mapF** and **reduceF** in [ii.go](src/main/ii.go) 主要有以下几个注意点
  - `strings.Join`是比较方面快速的字符串连接方法

```go
// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	fmt.Printf("document is:  %s\n", document)

	words := strings.FieldsFunc(value,func(c rune) bool {
		return !unicode.IsLetter(c)})
	for _, w := range words {
		kv := mapreduce.KeyValue{w, document}
		res = append(res, kv)
	}
	//fmt.Printf("the result of map is %s\n",res)
	return res
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	values = RemoveRepeatedElement(values)
	s := strings.Join(values, ",")
	s = strconv.Itoa(len(values)) +" "+ s
	return s
}
func RemoveRepeatedElement(arr []string) (newArr []string) {
	newArr = make([]string, 0)
	sort.Strings(arr)
	for i := 0; i < len(arr); i++ {
		repeat := false
		for j := i + 1; j < len(arr); j++ {
			if arr[i] == arr[j] {
				repeat = true
				break
			}
		}
		if !repeat {
			newArr = append(newArr, arr[i])
		}
	}
	return
}
```





