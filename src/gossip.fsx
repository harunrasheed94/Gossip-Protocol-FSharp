#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open System.Diagnostics
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Collections.Generic

let system = ActorSystem.Create("System")
let myClock = Stopwatch()
let rnd  = System.Random()
let finishedActors = new List<int>()
type DUnions = 
    | SetTopAlgo of int * IActorRef[] * string * int
    | SetNeighbors of int32 * IActorRef * IActorRef[]
    | InitiateGossip of string
    | GossipConvergence of int32
    | PushSum of Double * Double * Double
    | PushSumConvergence of Double * Double
    | BeginPushSum of Double

//Manager actor
let Manager (mailbox:Actor<_>) = 

    let mutable nodes = 0
    let mutable nodesRec = 0
    let mutable gridSize = 0
    let mutable allNodes:IActorRef[] = [||]

    let rec loop () = actor {
        let! msg = mailbox.Receive()
        match msg with
        | SetTopAlgo (n,allActors,algo,gridS) -> 
                nodes <- n
                gridSize <- gridS
                allNodes <- allActors

        | PushSumConvergence (s,w)->
             printfn "Convergence Time is %d ms" myClock.ElapsedMilliseconds
             Environment.Exit 0
        
        | GossipConvergence idx->

                if not(finishedActors.Contains(idx)) then
                    nodesRec <- nodesRec + 1
                    finishedActors.Add(idx)

                if nodesRec = nodes then
                   printfn "Convergence Time is %d ms" myClock.ElapsedMilliseconds
                   Environment.Exit 0
                
               //  let mutable randomIdx = rnd.Next(0,allNodes.Length)
               //  while randomIdx = idx do
               //    randomIdx <- rnd.Next(0,allNodes.Length)
               //  allNodes.[randomIdx] <! InitiateGossip("hello")

                          
        | _-> () 
        return! loop()
    }
    loop()

//Node actor function
let Node actorNum manager (mailbox:Actor<_>) =
    
    let mutable s = actorNum |> float
    let mutable w = 1.0
    let mutable noOfRounds = 1
    let mutable neighbors:IActorRef[]=[||]
    let mutable myRef:IActorRef = null
    let mutable idx:int32 = 0
    let mutable rumors = 0
   // let minimumDiff:Double = 10 ** -10.0

    let rec loop() = actor {
        let! msg = mailbox.Receive()
        match msg with 
        //Set the values
        | SetNeighbors (ind,ref,actorRef) ->
            idx <- ind
            myRef <- ref
            neighbors <- actorRef
         //Begin gossip protocol  
        | InitiateGossip gossip ->
           if rumors < 10 then 
                rumors <- rumors + 1 
                
           if rumors = 10 then
                manager <! GossipConvergence(idx) 

         //   if rumors < 10 then    
         //       let len = neighbors.Length
         //       let mutable randomIdx = rnd.Next(0,len)
         //       while randomIdx = idx do
         //         randomIdx <- rnd.Next(0,len)
         //       neighbors.[randomIdx] <! InitiateGossip("gossip")
           let len = neighbors.Length
           let mutable randomIdx = rnd.Next(0,len)
           while randomIdx = idx do
             randomIdx <- rnd.Next(0,len)
           neighbors.[randomIdx] <! InitiateGossip(gossip)  
                   
        
        | PushSum (s1,w1,minimumDiff) -> 
            
           let receiveSum = s + s1
           let receiveWeight = w + w1
           let len = neighbors.Length
           let randomIdx = rnd.Next(0,len)
           let diff =  (s/w - receiveSum/receiveWeight) |> abs
           
           if (diff > minimumDiff) then   
               s <- (s+s1)/2.0
               w <- (w+w1)/2.0
               noOfRounds <- 1
               neighbors.[randomIdx] <! PushSum(s,w,minimumDiff)
            elif (diff < minimumDiff && noOfRounds = 3) then
              // printfn "Before %A" (s/w)
               //printfn "After %A" (receiveSum/receiveWeight)
               manager <! PushSumConvergence(s,w)
            else
               s <- (s+s1)/2.0
               w <- (w+w1)/2.0
               noOfRounds <- noOfRounds+1
               neighbors.[randomIdx] <! PushSum(s,w,minimumDiff)   

        | BeginPushSum minimumDiff ->
                let index= rnd.Next(0,neighbors.Length)
                s<- s/2.0
                w <-w/2.0
                neighbors.[index] <! PushSum(s,w,minimumDiff)
              
        | _-> () 
        return! loop()   
    } 
    loop()

//function to start the respective algorithm
let beginProtocol (algo:string,num:int32,actors:IActorRef[]) = 
         let randomStart = rnd.Next(0,num-1)
         myClock.Start()
         if algo = "gossip" then
            printfn "Starting Gossip"
            actors.[randomStart] <! InitiateGossip("gossip")
         else
            printfn "Starting PushSum"
            actors.[randomStart] <! BeginPushSum(10.0 ** -10.0)  

//2d functions to find neighbors
//function to get top index for 2d   
let getTop i gridSize : int32 =

   let mutable index = -1
   
   if i-gridSize >= 0 then
      index <- i-gridSize
      
   index

//function to get bottom index for 2d 
let getBottom i gridSize : int32 =

   let mutable index = -1
   if i + gridSize < gridSize * gridSize then
      index <- i + gridSize
   index

//function to get left index for 2d
let getLeft i gridSize : int32 =

   let mutable index = -1
   if i % gridSize > 0 then 
      index <- i-1
   index

//function to get right index for 2d
let getRight i gridSize : int32 =

   let mutable index = -1
   if (i+1) % gridSize > 0 then
       index <- i + 1
   index   

//function to create the manager and nodes. Then set the neighbors according to the topology.
let setUpNetwork param = 
      let n,topology,algorithm,gridSize = param
      //spawn the manager
      let managerRef = Manager |> spawn system "Manager" 
    
      //spawn the actor nodes
      let actorsArr = (Array.init n (fun index -> Node (index+1) managerRef |> spawn system ("Node"+string(index+1))))
      managerRef <! SetTopAlgo(n,actorsArr,algorithm,gridSize)
      //set the neighbors according to the topology and start the protocol
      match topology with
      | "full" -> 
        for i in [0 .. n-1] do
          actorsArr.[i] <! SetNeighbors(i+1,actorsArr.[i],actorsArr)
           
        beginProtocol(algorithm,n,actorsArr)
        
      | "line" ->
         for i in [0 .. n-1] do
            let mutable neighborArr:IActorRef[] = [||]
            if i = 0 then 
               neighborArr <- [|actorsArr.[i+1]|] 
            elif i = n-1 then
               neighborArr <- [|actorsArr.[i-1]|]
            else 
               neighborArr <- [|actorsArr.[i-1]; actorsArr.[i+1]|]
            actorsArr.[i] <! SetNeighbors(i+1,actorsArr.[i],neighborArr)  

         beginProtocol(algorithm,n,actorsArr)

      | "2D" ->
          for i in [0 .. n-1] do
            let mutable neighborArr:IActorRef[] = [||]
             //get Top neighbor
            let top = getTop i gridSize 
            if top >= 0 then
               neighborArr <- Array.append neighborArr [|actorsArr.[top]|]
             
            //get Bottom neighbor
            let bottom = getBottom i gridSize
            if bottom >= 0 then
               neighborArr <- Array.append neighborArr [|actorsArr.[bottom]|]
       
             //get Left neighbor
            let left = getLeft i gridSize 
            if left >= 0 then
               neighborArr <- Array.append neighborArr [|actorsArr.[left]|]

             //get Right neighbor      
            let right = getRight i gridSize 
            if right >= 0 then
               neighborArr <- Array.append neighborArr [|actorsArr.[right]|]  
               
            actorsArr.[i] <! SetNeighbors(i+1,actorsArr.[i],neighborArr)

          beginProtocol(algorithm,n,actorsArr)

      | "imp2D" ->   
          for i in [0 .. n-1] do
            let neighborList = new List<int>()
            let mutable neighborArr:IActorRef[] = [||]
             //get Top neighbor
            let top = getTop i gridSize 
            if top >= 0 then
               neighborArr <- Array.append neighborArr [|actorsArr.[top]|]
               neighborList.Add(top)
             
            //get Bottom neighbor
            let bottom = getBottom i gridSize
            if bottom >= 0 then
               neighborArr <- Array.append neighborArr [|actorsArr.[bottom]|]
               neighborList.Add(bottom)
       
             //get Left neighbor
            let left = getLeft i gridSize 
            if left >= 0 then
               neighborArr <- Array.append neighborArr [|actorsArr.[left]|]
               neighborList.Add(left)

             //get Right neighbor      
            let right = getRight i gridSize 
            if right >= 0 then
               neighborArr <- Array.append neighborArr [|actorsArr.[right]|]
               neighborList.Add(right)
            
            //Add a random neighbor not part of existing neighbors for imp2D
            let mutable randomNeighbor = rnd.Next(0,n-1)
            while neighborList.Contains(randomNeighbor) do
                randomNeighbor <- rnd.Next(0,n-1)   
            neighborArr <- Array.append neighborArr [|actorsArr.[randomNeighbor]|]

            //set the neighbors
            actorsArr.[i] <! SetNeighbors(i+1,actorsArr.[i],neighborArr)

          beginProtocol(algorithm,n,actorsArr)
      | _-> ()   
          


//Read the inputs      
let args : string array = fsi.CommandLineArgs |> Array.tail
let mutable nNodes = args.[0] |> int
let topology=args.[1] 
let algorithm = args.[2]
let mutable gridSize = 0
//if topology is 2d or imp2d then make nodes as perfect square
if topology = "2D" || topology = "imp2D" then 
    gridSize <- floor (float(args.[0]) |> sqrt) |> int
    nNodes <- gridSize * gridSize

setUpNetwork(nNodes,topology,algorithm,gridSize)
System.Console.ReadLine() |> ignore 

