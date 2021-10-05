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
let numberOfNodes = 10

type Instructions =
    | NeighbourInitialization of IActorRef []
    | CallSelf
    | CallNeighbour
    | CountReached
    | StartTimer of int
    | TotalNodes of int

let Observer totalNodes (timer : Stopwatch) (mailbox: Actor<_>) = 
    let mutable count = 0
    let mutable startTime = 0

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        match msg with
        | CountReached ->
            count <- count + 1
            printfn "Count in observer %i %i" count totalNodes
            if count = totalNodes then
                printf "Inside terminate block"
                timer.Stop()
                printfn "Time taken for convergence : %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | StartTimer startTiming -> startTime <- startTiming
        | _ -> ()
        return! loop()
    }
    loop()




let Worker (dictionary : Dictionary<IActorRef, bool>) observer numberOfNodes (mailbox: Actor<_>) =
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
                // printfn "%i %A" random neighboursMap
                // while (neighboursMap.Item(neighbours.[random]) = 0) do
                //         random <- Random().Next(0,neighbours.Length) 
                



                if not dictionary.[neighbours.[random]] then
                    
                    // neighboursMap.Add(neighbours.[random], neighboursMap.Item(neighbours.[random]) - 1) |> ignore

                    neighbours.[random] <! CallNeighbour
                else
                    let mutable counter = 0
                    let mutable x = true
                    let mutable i = 0
                    while x do
                        if dictionary.[neighbours.[i]] then
                            counter <- counter+1
                        else 
                            x <- false
                        if counter = neighbours.Length then
                            x <- false
                            mailbox.Self <! CallNeighbour

                        i <- i + 1
                // Thread.Sleep(100)
                mailbox.Self <! CallSelf

        | CallNeighbour ->
            //printfn "Call Neighbour %A : listencount = %i" mailbox.Self.Path.Name listenCount
            if listenCount = 0 then
                mailbox.Self <! CallSelf
            if listenCount = 10 then
                // printf "%A limit reached " mailbox.Self.Path.Name
                printfn "Limit reached : %A : listencount = %i" mailbox.Self.Path.Name listenCount
                observer <! CountReached
                dictionary.[mailbox.Self] <- true
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
    for node in 0..numberOfNodes do
        let mutable neighbourList = [||]
        if node = 0 then do
            neighbourList <- (Array.append neighbourList[|nodeArray.[numberOfNodes-1] |])
        elif node = numberOfNodes-1 then do
            neighbourList <- (Array.append neighbourList[|nodeArray.[0] |])
        else
            neighbourList <- (Array.append neighbourList[|nodeArray.[0] |])
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
    
    
    let mutable  nodeArray = [||]
    let dictionary = new Dictionary<IActorRef, bool>()
    //creating nodes and initialiazing their neighbours
    nodeArray <- Array.zeroCreate(numberOfNodes + 1)
    for x in [0 .. numberOfNodes] do
        let actorName: string= "node" + string(x)
        let WorkeractorRef = spawn gossipSystem actorName (Worker dictionary observer numberOfNodes)
        nodeArray.[x] <- WorkeractorRef
        dictionary.Add(WorkeractorRef, false)

    createTopologies numberOfNodes topology nodeArray
    
    let intitialNode = Random().Next(0, numberOfNodes - 1)
    timer.Start()
    observer <! StartTimer(DateTime.Now.TimeOfDay.Milliseconds)
    nodeArray.[intitialNode] <! CallSelf// return an integer exit code
    System.Console.ReadKey() |> ignore
    0