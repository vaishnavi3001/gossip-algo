#if INTERACTIVE
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"
#endif

open System
open Akka.Actor
open Akka.FSharp
open System.Diagnostics


//notes: initialize scheduler for each actor when hop count is 0
// don't use dictionary. keep transmitting to neighbour irrespective of neighbour's hop count. if neighbour has reached his limit he won't transmit
// stop transmitting only when own hop count is ten

type Instructions =
    | NeighbourInitialization of IActorRef []
    | CallFromSelf
    | CallFromNeighbour
    | CountReached
    | StartTimer of int
    | TotalNodes of int
    | NodeReachedOnce of string
    | SumReached of Double * Double
    | ClearLists
    | SendSum of Double * Double * Double
    | IntializeScheduler

let Observer totalNodes (timer : Stopwatch) (mailbox: Actor<_>) = 
    let mutable count = 0
    let mutable startTime = 0
    let mutable reachedCount = 0
    let mutable pushsumCount = 0

    let rec loop() = actor{
        let! msg = mailbox.Receive();
        let sender = mailbox.Sender()
        match msg with
        | CountReached ->
            count <- count + 1
            printfn "Count in observer %i %i and converged node is %s" count totalNodes sender.Path.Name
            if count = totalNodes then
                printf "Inside terminate block"
                let timeNow = System.DateTime.Now.TimeOfDay.TotalMilliseconds 
                printfn "Stop system Time: %A" timeNow
                timer.Stop()
                printfn "Time taken for convergence : %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | NodeReachedOnce actorName ->
            reachedCount <- reachedCount + 1
            if reachedCount = totalNodes then
                printfn "all nodes have received information, system has converged"
                let timeNow = System.DateTime.Now.TimeOfDay.TotalMilliseconds 
                printfn "Stop system converge Time: %A" timeNow
                timer.Stop()
                printfn "Time taken for convergence : %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | SumReached (s: Double, w: Double)->
            pushsumCount <- pushsumCount + 1
            printfn "%s has converged with values s = %f and w = %f and s/w = %f and totalcount =%i" sender.Path.Name s w (s/w) pushsumCount
            if pushsumCount = totalNodes then
                printfn "System has converged"
                Environment.Exit(0)

        | StartTimer startTiming -> startTime <- startTiming
        | _ -> ()
        return! loop()
    }
    loop()




let Worker observer numberOfNodes initialWeight delta (gossipSystem : ActorSystem) (mailbox: Actor<_>) =
    let mutable listenCount = 0
    let mutable neighbours: IActorRef [] = [||]
    let mutable sum = initialWeight |> double
    let mutable weight = 1.0
    let mutable sameRatioRound = 0
    let mutable sumList = []
    let mutable weightList = []
    let mutable round = 0
    let mutable totalSum = 0.0
    let mutable totalWeight = 0.0
    let mutable converged = 0

    let rec loop() = actor{
        if converged = 0 then
            let! message = mailbox.Receive();
            let sender = mailbox.Sender();
            match message with
            | NeighbourInitialization neighbourlist ->
                neighbours <- neighbourlist


            | CallFromSelf ->
                if listenCount < 11 then
                    let mutable random = Random().Next(0,neighbours.Length)
                    neighbours.[random] <! CallFromNeighbour
                    //.Thread.Sleep(5)
                    //mailbox.Self <! CallSelf

            | CallFromNeighbour ->
                if listenCount = 0 then
                    // initialize scheduler here. it will only run once for each actor.
                    gossipSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(25.0), mailbox.Self, CallFromSelf)
                    observer <! NodeReachedOnce mailbox.Self.Path.Name
                if listenCount = 10 then
                    observer <! CountReached
                listenCount <- listenCount + 1

           
            | ClearLists ->
               
                totalSum <- 0.0
                totalWeight <- 0.0
                for x in sumList do
                    totalSum <- totalSum + x
                for x in weightList do
                    totalWeight <- totalWeight + x

                let oldRatio = sum / weight
                sum <- totalSum
                weight <- totalWeight
                let newRatio = sum / weight

               
                let mutable random = Random().Next(0,neighbours.Length)
                neighbours.[random] <! SendSum (sum / 2.0, weight / 2.0, delta)
                mailbox.Self <! SendSum (sum / 2.0, weight / 2.0, delta)


                let gap = newRatio - oldRatio |> abs
                if gap > delta then
                    sameRatioRound <- 0
                else
                    sameRatioRound <- sameRatioRound + 1
                if sameRatioRound = 6 then
                    observer <! SumReached (sum, weight)
                    converged <- 1
                sumList <- []
                weightList <- []
                round <- round + 1
                

            | SendSum (s, w, delta) ->
                sumList <- sumList @ [s]
                weightList <- weightList @ [w]

            | IntializeScheduler ->
                mailbox.Self <! SendSum (sum , weight, delta)
                gossipSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(25.0), mailbox.Self, ClearLists)
                


            return! loop()
        }
    loop()



let fullTopology numberOfNodes (nodeArray: IActorRef [])= 
    for node in 0..numberOfNodes-1 do
        let mutable neighbourList = [||]
        for neighbourNode in 0..numberOfNodes-1 do
            if node <> neighbourNode then
                neighbourList <- (Array.append neighbourList[|nodeArray.[neighbourNode] |])
        nodeArray.[node] <! NeighbourInitialization(neighbourList)

let lineTopology numberOfNodes (nodeArray: IActorRef [])= 
    for node in 0..numberOfNodes-1 do
        let mutable neighbourList = [||]
        if node <> 0 && node <> numberOfNodes-1 then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node-1] ; nodeArray.[node+1]|])
        if node = 0 then 
            neighbourList <- (Array.append neighbourList[|nodeArray.[node+1]|])
        if node = numberOfNodes-1 then 
            neighbourList <- (Array.append neighbourList[|nodeArray.[node-1]|])

        nodeArray.[node] <! NeighbourInitialization(neighbourList)

let threeDTopology numberOfNodes (nodeArray: IActorRef [])=
    let cubeRoot:int = int(Math.Floor(Math.Cbrt(float(numberOfNodes))))
    let sqr = cubeRoot * cubeRoot
    for node in 0..numberOfNodes-1 do
        let mutable neighbourList = [||]
        
        // left node
        if node % cubeRoot <> 0 then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node-1]|])
        
        // right node
        if node % cubeRoot <> cubeRoot-1 then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node+1]|])
        
        // top
        if node % sqr >= cubeRoot  then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node-cubeRoot]|])

        // bottom
        if node % sqr < (cubeRoot-1)*cubeRoot  then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node+cubeRoot]|])

        // frontPlane
        if node-sqr >= 0 then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node-sqr]|])
        
        // back plane
        if node+sqr < numberOfNodes then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node+sqr]|])

        nodeArray.[node] <! NeighbourInitialization(neighbourList)

let imperfectThreeDTopology numberOfNodes (nodeArray: IActorRef [])=
    let cubeRoot:int = int(Math.Floor(Math.Cbrt(float(numberOfNodes))))
    let sqr = cubeRoot * cubeRoot
    
    for node in 0..numberOfNodes-1 do
        let randomNode = Random().Next(0, numberOfNodes - 1)
        let mutable neighbourList = [||]
        
        // left node
        if node % cubeRoot <> 0 then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node-1]|])
        
        // right node
        if node % cubeRoot <> cubeRoot-1 then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node+1]|])
        
        // top
        if node % sqr >= cubeRoot  then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node-cubeRoot]|])

        // bottom
        if node % sqr < (cubeRoot-1)*cubeRoot  then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node+cubeRoot]|])

        // frontPlane
        if node-sqr >= 0 then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node-sqr]|])
        
        // back plane
        if node+sqr < numberOfNodes then
            neighbourList <- (Array.append neighbourList[|nodeArray.[node+sqr]|])
        
        neighbourList <- (Array.append neighbourList[|nodeArray.[randomNode]|])
         
        nodeArray.[node] <! NeighbourInitialization(neighbourList)
        

let createTopologies numberOfNodes topology nodeArray= 
    // topologyDict.Add(1, [2])
    match topology with
    | "full" -> fullTopology numberOfNodes nodeArray
    | "3D" -> threeDTopology numberOfNodes nodeArray
             
    | "line" -> lineTopology numberOfNodes nodeArray

    | "imp3D" -> imperfectThreeDTopology numberOfNodes nodeArray
    | _ -> 
        printfn "Not a valid Topology%A" topology
        

[<EntryPoint>]
let main argv =
    let mutable numberOfNodes =  (int) argv.[0]
    let topology = (string) argv.[1]
    match topology with
    | "imp3D" -> 
        let cubeRoot:int = int(Math.Floor(Math.Cbrt(float(numberOfNodes))))
        numberOfNodes <- cubeRoot * cubeRoot * cubeRoot

    | "3D" -> 
        let cubeRoot:int = int(Math.Floor(Math.Cbrt(float(numberOfNodes))))
        numberOfNodes <- cubeRoot * cubeRoot * cubeRoot

    | _ ->()

    let algo = (string) argv.[2]
    let gossipSystem = ActorSystem.Create("GossipSystem")
    let timer = Diagnostics.Stopwatch()
    let observer = spawn gossipSystem "Observer" (Observer numberOfNodes timer)

    let mutable  nodeArray = [||]

    nodeArray <- Array.zeroCreate(numberOfNodes)
    for x in [0 .. numberOfNodes - 1] do
        let actorName: string= "node" + string(x)
        let WorkeractorRef = spawn gossipSystem actorName (Worker observer numberOfNodes (x) (10.0 ** -10.0) gossipSystem)
        nodeArray.[x] <- WorkeractorRef
    printfn " after creating actors %A" System.DateTime.Now.TimeOfDay.TotalMilliseconds
    createTopologies numberOfNodes topology nodeArray
    printfn " after creating topologies %A" System.DateTime.Now.TimeOfDay.TotalMilliseconds
    let intitialNode = Random().Next(0, numberOfNodes - 1)
    timer.Start()
    let timeNow = System.DateTime.Now.TimeOfDay.TotalMilliseconds 
    printfn "Start system Time: %A" timeNow
    observer <! StartTimer(DateTime.Now.TimeOfDay.Milliseconds)
    match algo with
    | "gossip" ->
        nodeArray.[intitialNode] <! CallFromNeighbour

    | "pushsum" ->
        for x in nodeArray do
            x <! IntializeScheduler

    | _ -> printfn("invalid algorithm")

    System.Console.ReadKey() |> ignore
    printfn "Exiting"
    0