#if INTERACTIVE
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"
#endif

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Diagnostics
open System.Collections.Generic
open System.Threading

// let timer = Diagnostics.Stopwatch()
// let gossipSystem = ActorSystem.Create("GossipSystem")
// let mutable  nodeArray = [||]
// let dictionary = new Dictionary<IActorRef, bool>()

//notes: initialize scheduler for each actor when hop count is 0
// don't use dictionary. keep transmitting to neighbour irrespective of neighbour's hop count. if neighbour has reached his limit he won't transmit
// stop transmitting only when own hop count is ten
// let numberOfNodes = 10

type Instructions =
    | NeighbourInitialization of IActorRef []
    | InitializePushSum of int
    | CallSelf
    | CallNeighbour
    | CountReached
    | StartTimer of int
    | TotalNodes of int
    | NodeReachedOnce of string
    | CallSelfPushSum of Double
    | ComputePushSum of Double * Double * Double
    | PushSumResult of Double * Double

let Observer totalNodes (timer : Stopwatch) (mailbox: Actor<_>) = 
    let mutable count = 0
    let mutable startTime = 0
    let mutable reachedCount = 0

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        match msg with
        | CountReached ->
            count <- count + 1
            // printfn "Count in observer %i %i" count totalNodes
            if count = totalNodes then
                printf "Inside terminate block"
                let timeNow = System.DateTime.Now.TimeOfDay.TotalMilliseconds 
                printfn "Stop system Time: %A" timeNow
                timer.Stop()
                printfn "Time taken for convergence : %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | NodeReachedOnce actorName ->
            // printfn "%s actor has received information" actorName
            reachedCount <- reachedCount + 1
            if reachedCount = totalNodes then
                printfn "all nodes have received information, system has converged"
                let timeNow = System.DateTime.Now.TimeOfDay.TotalMilliseconds 
                printfn "Stop system converge Time: %A" timeNow
                timer.Stop()
                
                printfn "Time taken for convergence : %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | PushSumResult (sum, weight) ->
            let result = sum/(weight)
            printfn "result%A" result 
            count <- count + 1
            if count = totalNodes then
                timer.Stop()
                
                printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)

        | StartTimer startTiming -> startTime <- startTiming
        | _ -> ()
        return! loop()
    }
    loop()




let Worker observer numberOfNodes WorkerID (gossipSystem : ActorSystem) (mailbox: Actor<_>) =
    let mutable listenCount = 0
    let mutable neighbours: IActorRef [] = [||]
    // let mutable neighboursMap = Map.empty
    let mutable sum = double(WorkerID) |>double
    let mutable weight = 1.0
    let mutable termRound = 1
    let mutable converged = false
    let mutable alreadyConverged = false

    let rec loop() = actor{
        let! message = mailbox.Receive();
        match message with
        
        | NeighbourInitialization neighbourlist ->
            neighbours <- neighbourlist
            // for n in neighbourlist do
            //     neighboursMap.Add(n, 10 / neighbourlist.Length) |> ignore

        | CallSelf ->
            //printfn "%A call self actor called" mailbox.Self.Path.Name
            if listenCount < 11 then
                let mutable random = Random().Next(0,neighbours.Length)
                        
                neighbours.[random] <! CallNeighbour
                //.Thread.Sleep(5)
                //mailbox.Self <! CallSelf

        | CallNeighbour ->
            //printfn "Call Neighbour %A : listencount = %i" mailbox.Self.Path.Name listenCount
            if listenCount = 0 then
                // initialize scheduler here. it will only run once for each actor.
                //system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), mailbox.Self, PushsumObjSelf(actorPool, bossRef))  
                gossipSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(1.0),TimeSpan.FromMilliseconds(1.0), mailbox.Self, CallSelf)
                observer <! NodeReachedOnce mailbox.Self.Path.Name
                //mailbox.Self <! CallSelf
            if listenCount = 10 then
                // printf "%A limit reached " mailbox.Self.Path.Name
                // printfn "Limit reached : %A : listencount = %i" mailbox.Self.Path.Name listenCount
                observer <! CountReached
                //dictionary.[mailbox.Self] <- true
            listenCount <- listenCount + 1

        | InitializePushSum (num: int) ->
            sum <- num |> double

        | CallSelfPushSum delta ->
            let index = Random().Next(0, neighbours.Length)

            sum <- sum / 2.0
            weight <- weight / 2.0
            neighbours.[index] <! ComputePushSum(sum, weight, delta)

        | ComputePushSum (s: float, w, delta) ->
            
            let newsum = sum + s
            let newweight = weight + w

            let cal = sum / weight - newsum / newweight |> abs

            if alreadyConverged then
                let index = Random().Next(0, neighbours.Length)
                neighbours.[index] <! ComputePushSum(s, w, delta)
            
            else
                if cal > delta then
                    termRound <- 0
                    mailbox.Self <! ComputePushSum(sum, weight, delta)
                else 
                    termRound <- termRound + 1

                if  termRound = 3 then
                    termRound <- 0
                    alreadyConverged <- true
                    observer <! PushSumResult(sum, weight)
            
                sum <- newsum / 2.0
                weight <- newweight / 2.0
                // let index = Random().Next(0, neighbours.Length)
                // neighbours.[index] <! ComputePushSum(sum, weight, delta)
        | _ -> ()

        return! loop()
    }
    loop()



let fullTopology numberOfNodes (nodeArray: IActorRef [])= 
    for node in 0..numberOfNodes do
        let mutable neighbourList = [||]
        for neighbourNode in 0..numberOfNodes-1 do
            if node <> neighbourNode then
                neighbourList <- (Array.append neighbourList[|nodeArray.[neighbourNode] |])
        nodeArray.[node] <! NeighbourInitialization(neighbourList)
        nodeArray.[node] <! InitializePushSum (node: int)

let lineTopology numberOfNodes (nodeArray: IActorRef [])= 
    for node in 0..numberOfNodes-1 do
        let mutable neighbourList = [||]
        if node = 0 then do
            neighbourList <- (Array.append neighbourList[|nodeArray.[numberOfNodes-1] |])
        elif node = numberOfNodes-1 then do
            neighbourList <- (Array.append neighbourList[|nodeArray.[0]; nodeArray.[numberOfNodes-2] |])
        else
            neighbourList <- (Array.append neighbourList[|nodeArray.[node-1] ; nodeArray.[node+1]|])
        nodeArray.[node] <! NeighbourInitialization(neighbourList) //only in case of gossip
        nodeArray.[node] <! InitializePushSum (node: int) //only in case of push sum

let createTopologies numberOfNodes topology nodeArray= 
    // topologyDict.Add(1, [2])
    match topology with
    | "full" -> fullTopology numberOfNodes nodeArray
    // | "3D" -> threeDTopology numNodes nodeArray
    | "line" -> lineTopology numberOfNodes nodeArray
    // | "imp3D" -> imperfectThreeDTopology numNodes nodeArray
    | _ -> 
        printfn "Not a valid Topology%A" topology
        

[<EntryPoint>]
let main argv =
    let numberOfNodes =  (int) argv.[0]
    let topology = (string) argv.[1]
    let protocol = (string) argv.[2]
    let gossipSystem = ActorSystem.Create("GossipSystem")
    
    let timer = Diagnostics.Stopwatch()
    
    let observer = spawn gossipSystem "Observer" (Observer numberOfNodes timer)
    printfn " after declaring variables %A" System.DateTime.Now.TimeOfDay.TotalMilliseconds

    let mutable  nodeArray = [||]
    
    //creating nodes and initialiazing their neighbours
    nodeArray <- Array.zeroCreate(numberOfNodes + 1)
    for x in [0 .. numberOfNodes] do
        let actorName: string= "node" + string(x)
        let WorkeractorRef = spawn gossipSystem actorName (Worker observer numberOfNodes x gossipSystem)
        nodeArray.[x] <- WorkeractorRef
    printfn " after creating actors %A" System.DateTime.Now.TimeOfDay.TotalMilliseconds

    //timer.Start()
    createTopologies numberOfNodes topology nodeArray
    printfn " after creating topologies %A" System.DateTime.Now.TimeOfDay.TotalMilliseconds

    //timer.Stop()
    let intitialNode = Random().Next(0, numberOfNodes - 1)
    timer.Start()
    let timeNow = System.DateTime.Now.TimeOfDay.TotalMilliseconds 
    printfn "Start system Time: %A" timeNow
    observer <! StartTimer(DateTime.Now.TimeOfDay.Milliseconds)

    match protocol with 
    | "gossip" -> 
        nodeArray.[intitialNode] <! CallSelf // return an integer exit code
    | "push-sum" -> 
        nodeArray.[intitialNode] <! CallSelfPushSum(10.0 ** -10.0) // return an integer exit code
    | _ -> 
        printfn "Invalid topology"

    System.Console.ReadKey() |> ignore
    0