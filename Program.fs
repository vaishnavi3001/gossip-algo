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
let numberOfNodes = 10

type Instructions =
    | NeighbourInitialization of IActorRef []
    | CallSelf
    | CallNeighbour
    | CountReached
    | StartTimer of int
    | TotalNodes of int
    | NodeReachedOnce of string

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

        | StartTimer startTiming -> startTime <- startTiming
        | _ -> ()
        return! loop()
    }
    loop()




let Worker observer numberOfNodes (gossipSystem : ActorSystem) (mailbox: Actor<_>) =
    let mutable listenCount = 0
    let mutable neighbours: IActorRef [] = [||]
    // let mutable neighboursMap = Map.empty
    let mutable converged = false

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
                gossipSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(25.0), mailbox.Self, CallSelf)
                observer <! NodeReachedOnce mailbox.Self.Path.Name
                //mailbox.Self <! CallSelf
            if listenCount = 10 then
                // printf "%A limit reached " mailbox.Self.Path.Name
                // printfn "Limit reached : %A : listencount = %i" mailbox.Self.Path.Name listenCount
                observer <! CountReached
                //dictionary.[mailbox.Self] <- true
            listenCount <- listenCount + 1


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

let lineTopology numberOfNodes (nodeArray: IActorRef [])= 
    for node in 0..numberOfNodes-1 do
        let mutable neighbourList = [||]
        if node = 0 then do
            neighbourList <- (Array.append neighbourList[|nodeArray.[numberOfNodes-1] |])
        elif node = numberOfNodes-1 then do
            neighbourList <- (Array.append neighbourList[|nodeArray.[0]; nodeArray.[numberOfNodes-2] |])
        else
            neighbourList <- (Array.append neighbourList[|nodeArray.[node-1] ; nodeArray.[node+1]|])
        nodeArray.[node] <! NeighbourInitialization(neighbourList)

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
    let gossipSystem = ActorSystem.Create("GossipSystem")
    let timer = Diagnostics.Stopwatch()
    let observer = spawn gossipSystem "Observer" (Observer numberOfNodes timer)
    //printfn "%i" numberOfNodes
    printfn " after declaring variables %A" System.DateTime.Now.TimeOfDay.TotalMilliseconds
    let mutable  nodeArray = [||]
    //let dictionary = new Dictionary<IActorRef, bool>()
    //creating nodes and initialiazing their neighbours
    nodeArray <- Array.zeroCreate(numberOfNodes + 1)
    for x in [0 .. numberOfNodes] do
        let actorName: string= "node" + string(x)
        let WorkeractorRef = spawn gossipSystem actorName (Worker observer numberOfNodes gossipSystem)
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
    nodeArray.[intitialNode] <! CallSelf// return an integer exit code
    printfn "test"
    System.Console.ReadKey() |> ignore
    0