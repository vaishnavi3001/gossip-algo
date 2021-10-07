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

type Instructions =
    | NeighbourInitialization of IActorRef []
    | CallFromSelf
    | CallFromNeighbour
    | CountReached
    | StartTimer of int
    | TotalNodes of int
    | NodeReachedOnce of string
    | CallFromSelfPushSome
    | CallFromNeighbourPushSum of Double * Double * Double
    | SumReached of Double * Double
    | ClearLists
    | SendSum of Double * Double * Double
    | IntializeScheduler

let Observer totalNodes (timer : Stopwatch) (mailbox: Actor<_>) = 
    let mutable count = 0
    let mutable startTime = 0
    let mutable reachedCount = 0
    let mutable pushSomeCount = 0

    let rec loop() = actor{
        //printfn "OBSERVER START"
        let! msg = mailbox.Receive();
        let sender = mailbox.Sender()
        //printfn "MSG RECEIVED"
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
        | SumReached (s: Double, w: Double)->
            pushSomeCount <- pushSomeCount + 1
            printfn "%s has converged with values s = %f and w = %f and s/w = %f" sender.Path.Name s w (s/w)
            //printfn "OB %d %s %d BO" pushSomeCount sender.Path.Name totalNodes
            if pushSomeCount = totalNodes then
                printfn "System has converged"
                Environment.Exit(0)

        | StartTimer startTiming -> startTime <- startTiming
        | _ -> ()
        //printfn "OBSERVER RETURN"
        return! loop()
    }
    loop()




let Worker observer numberOfNodes initialWeight delta (gossipSystem : ActorSystem) (mailbox: Actor<_>) =
    let mutable listenCount = 0
    let mutable neighbours: IActorRef [] = [||]
    let mutable sum = initialWeight |> double
    let mutable weight = 1.0
    let mutable oldsum = sum
    let mutable oldweight = weight
    let mutable sameRatioRound = 0
    let mutable initialCall = 0
    // let mutable neighboursMap = Map.empty
    let mutable converged = false
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
            //printfn "ACTOR MSG RECEI %s %A" mailbox.Self.Path.Name message
            //Thread.Sleep(1000)
            match message with
            
            | NeighbourInitialization neighbourlist ->
                neighbours <- neighbourlist


            | CallFromSelf ->
                //printfn "%A call self actor called" mailbox.Self.Path.Name
                if listenCount < 11 then
                    let mutable random = Random().Next(0,neighbours.Length)
                    neighbours.[random] <! CallFromNeighbour
                    //.Thread.Sleep(5)
                    //mailbox.Self <! CallSelf

            | CallFromNeighbour ->
                //printfn "Call Neighbour %A : listencount = %i" mailbox.Self.Path.Name listenCount
                if listenCount = 0 then
                    // initialize scheduler here. it will only run once for each actor.
                    //system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.0), mailbox.Self, PushsumObjSelf(actorPool, bossRef))  
                    gossipSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(25.0), mailbox.Self, CallFromSelf)
                    observer <! NodeReachedOnce mailbox.Self.Path.Name
                    //mailbox.Self <! CallSelf
                if listenCount = 10 then
                    // printf "%A limit reached " mailbox.Self.Path.Name
                    // printfn "Limit reached : %A : listencount = %i" mailbox.Self.Path.Name listenCount
                    observer <! CountReached
                    //dictionary.[mailbox.Self] <- true
                listenCount <- listenCount + 1

            | CallFromSelfPushSome ->
                //printfn "%f %f"  sum weight
                let mutable random = Random().Next(0,neighbours.Length)
                sum <- sum / 2.0
                weight  <- weight / 2.0
                neighbours.[random] <! CallFromNeighbourPushSum (sum, weight, delta)
                // mailbox.Self <! CallFromNeighbourPushSum (sum, weight, delta)


            | CallFromNeighbourPushSum (s: float, w: float, delta) ->

                let updatedSum = sum + s
                let updatedWeight = weight + w
                let gap = updatedSum / updatedWeight - sum / weight |> abs
                printfn "S %f %f %f %f %f %f %s %s %f %d E" s w sum weight updatedSum updatedWeight mailbox.Self.Path.Name sender.Path.Name gap sameRatioRound
                Thread.Sleep(1000)

                if gap > delta then
                    sameRatioRound <- 0
                    sum <- updatedSum / 2.0
                    weight <- updatedWeight / 2.0
                else 
                    sameRatioRound <- sameRatioRound + 1

                if sameRatioRound = 3 then
                        printfn "Actor %s has converged with sum = %f and w = %f" mailbox.Self.Path.Name sum weight
                        observer <! SumReached
                    // TODO STOP SOMEHOW and if sent once to observer, dont send again
                let mutable random = Random().Next(0,neighbours.Length)
                neighbours.[random] <! CallFromNeighbourPushSum (sum, weight, delta)
                    // mailbox.Self <! CallFromNeighbourPushSum (sum, weight, delta)

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
                // if sameRatioRound < 3 then 
                //     neighbours.[random] <! SendSum (sum / 2.0, weight / 2.0, delta)
                //     mailbox.Self <! SendSum (sum / 2.0, weight / 2.0, delta)
                if (sumList.Length > 1) then

                    let gap = newRatio - oldRatio |> abs
                    if gap > delta then
                        sameRatioRound <- 0
                    else
                        sameRatioRound <- sameRatioRound + 1
                    if sameRatioRound = 3 then
                        //printfn "Actor %s has converged with sum = %f and w = %f at round %d" mailbox.Self.Path.Name sum weight round
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
                gossipSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(50.0), mailbox.Self, ClearLists)
                


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
    let algo = (string) argv.[2]
    let gossipSystem = ActorSystem.Create("GossipSystem")
    let timer = Diagnostics.Stopwatch()
    let observer = spawn gossipSystem "Observer" (Observer numberOfNodes timer)
    //printfn "%i" numberOfNodes
    //printfn " after declaring variables %A" System.DateTime.Now.TimeOfDay.TotalMilliseconds
    let mutable  nodeArray = [||]
    //let dictionary = new Dictionary<IActorRef, bool>()
    //creating nodes and initialiazing their neighbours
    nodeArray <- Array.zeroCreate(numberOfNodes + 1)
    for x in [0 .. numberOfNodes] do
        let actorName: string= "node" + string(x)
        let WorkeractorRef = spawn gossipSystem actorName (Worker observer numberOfNodes (x) (10.0 ** -10.0) gossipSystem)
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
    if (algo = "pushsome") then
        for x in nodeArray do
            x <! IntializeScheduler
        //nodeArray.[intitialNode] <! CallFromSelfPushSome
    else
        nodeArray.[intitialNode] <! CallFromSelf // return an integer exit code
    System.Console.ReadKey() |> ignore
    printfn "HELLO"
    0