package main

import (
    "fmt"
    "os"
    "strconv"
    "math"
	"encoding/csv"
    "sync"
)

type CensusGroup struct {
	population int
	latitude, longitude float64
}

func ParseCensusData(fname string) ([]CensusGroup, error) {
	file, err := os.Open(fname)
    if err != nil {
		return nil, err
    }
    defer file.Close()

	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, err
	}
	censusData := make([]CensusGroup, 0, len(records))

    for _, rec := range records {
        if len(rec) == 7 {
            population, err1 := strconv.Atoi(rec[4])
            latitude, err2 := strconv.ParseFloat(rec[5], 64)
            longitude, err3 := strconv.ParseFloat(rec[6], 64)
            if err1 == nil && err2 == nil && err3 == nil {
                latpi := latitude * math.Pi / 180
                latitude = math.Log(math.Tan(latpi) + 1 / math.Cos(latpi))
                censusData = append(censusData, CensusGroup{population, latitude, longitude})
            }
        }
    }

	return censusData, nil
}

func main () {
	if len(os.Args) < 4 {
		fmt.Printf("Usage:\nArg 1: file name for input data\nArg 2: number of x-dim buckets\nArg 3: number of y-dim buckets\nArg 4: -v1, -v2, -v3, -v4, -v5, or -v6\n")
		return
	}
	fname, ver := os.Args[1], os.Args[4]
    xdim, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println(err)
		return
	}
    ydim, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println(err)
		return
	}
	censusData, err := ParseCensusData(fname)
	if err != nil {
		fmt.Println(err)
		return
	}

    fmt.Println("consensus data is: ",censusData[0])

    // Some parts may need no setup code

    var leftLong, rightLong, lowerLat,upperLat float64 = 180.000, -180.000, 90.000, -90.000  
    cutOf:= 250000
    // rightCorner,lowerCorner,upperCorner float64 = 
    // done := make(chan bool);

    switch ver {
    case "-v1":

        fmt.Println("In case 1")
        for _, data := range censusData{
             leftLong,rightLong,lowerLat,upperLat= findCorners(leftLong,rightLong,lowerLat,upperLat,data);
        }
    case "-v2":
        fmt.Println("In case 2")
        simpleParallelCorners(&leftLong,&rightLong,&lowerLat,&upperLat,cutOf,censusData)

        // YOUR SETUP CODE FOR PART 2
    case "-v3":
            for _, data := range censusData{
             leftLong,rightLong,lowerLat,upperLat= findCorners(leftLong,rightLong,lowerLat,upperLat,data);
         }
        // YOUR SETUP CODE FOR PART 3
    case "-v4":
       simpleParallelCorners(&leftLong,&rightLong,&lowerLat,&upperLat,cutOf,censusData)
        // YOUR SETUP CODE FOR PART 4
    case "-v5":
       simpleParallelCorners(&leftLong,&rightLong,&lowerLat,&upperLat,cutOf,censusData)
        // YOUR SETUP CODE FOR PART 5
    case "-v6":
       simpleParallelCorners(&leftLong,&rightLong,&lowerLat,&upperLat,cutOf,censusData)

        // YOUR SETUP CODE FOR PART 6
    default:
        fmt.Println("Invalid version argument")
        return
    }

    fmt.Println("leftLong rightLong lowerLat upperLat: ",leftLong,rightLong,lowerLat,upperLat)
    fmt.Println("xdim ydim: ", xdim,ydim)



/////////-------------------------------------------------------/////////////////////

    for {    //To run infinitely and compute the result based in the input

        var west, south, east, north int
        fmt.Println("Please input values: ")
        n, err := fmt.Scanln(&west, &south, &east, &north)
        if n != 4 || err != nil || west<1 || west>xdim || south<1 || south>ydim || east<west || east>xdim || north<south || north>ydim {
            break
        }

        var population  int
        var percentage float64
        var totalPopulation int;
        xBlock := (rightLong - leftLong) / float64(xdim)
        yBlock := (upperLat - lowerLat) / float64(ydim)
        fmt.Println("xBlock, yclock: ",xBlock,yBlock)

        switch ver {
        case "-v1":

            for _, data := range censusData{

                totalPopulation+= data.population;

                xPosit,yPosit := func (data CensusGroup) (float64,float64){
                x:=((data.longitude - leftLong) / xBlock) + 1   
                y:=((data.latitude - lowerLat)/ yBlock) + 1
                return x, y;
                }(data)
                
                population=checkBlock(south,east,north,west,population,xPosit,yPosit,data)          
            }

            // population=firstVersion(leftLong,lowerLat,xBlock,yBlock,west,south,east,north,population,censusData)


        case "-v2":
                simpleParallelQuery(leftLong,lowerLat,&south,&east,&north,&west,&population, &totalPopulation,cutOf,xBlock,yBlock,censusData)
                fmt.Println("Total population: " , totalPopulation)

            // YOUR QUERY CODE FOR PART 2
        case "-v3":
            array := make([][]int, ydim+2)
            for i := range array {
                array[i] = make([]int, xdim+2)
            }
           

            for _, data := range censusData{    //First step

                totalPopulation+= data.population;

                xPosit,yPosit := func (data CensusGroup) (float64,float64){
                x:=((data.longitude-leftLong) / xBlock) + 1   
                y:=((data.latitude - lowerLat)/ yBlock) + 1
                return x, y;
                }(data)
                // fmt.Println(int(xPosit),int(yPosit))
                array[int(yPosit)][int(xPosit)]+=data.population       
            }

            grid := make([][]int, ydim+3)
            for i := range grid {
                grid[i] = make([]int, xdim+3)
               
                if i<ydim+2{
                fmt.Println(array[i])}
            }

             for y:=ydim+1;y>0;y--{         //Second step
                    for x:=1;x<xdim+2;x++{
                    grid[y][x] = array[y][x]+grid[y][x-1]+grid[y+1][x]-grid[y+1][x-1]
                }

                      // fmt.Println("array: ",array[y])
                    // fmt.Println("grid: ",grid[y])
                // fmt.Println("grid: ",grid[x])
            }
            population= grid[south][east] - grid[south][west-1] -grid[north+1][east] + grid[north+1][west-1]


            // YOUR QUERY CODE FOR PART 3
        case "-v4":
                 array := make([][]int, ydim+2)
            for i := range array {
                array[i] = make([]int, xdim+2)
            }
            // fmt.Println("length: ",len(array))

            var mut sync.Mutex;

            array=ver4Parallel(leftLong,lowerLat,xBlock,yBlock,&totalPopulation, cutOf,xdim,ydim,array,censusData,&mut)

            grid := make([][]int, ydim+3)
            for i := range grid {
                grid[i] = make([]int, xdim+3)
               
                if i<ydim+2{
                fmt.Println(array[i])}
            }

             for y:=ydim+1;y>0;y--{         //Second step
                    for x:=1;x<xdim+2;x++{
                    grid[y][x] = array[y][x]+grid[y][x-1]+grid[y+1][x]-grid[y+1][x-1]
                }
            }
            population= grid[south][east] - grid[south][west-1] -grid[north+1][east] + grid[north+1][west-1]   //Query

            // YOUR QUERY CODE FOR PART 4
        case "-v5":

             array := make([][]int, ydim+2)
            for i := range array {
                array[i] = make([]int, xdim+2)
            }

            mutArray := make([][]sync.Mutex , ydim+2)
              for i := range mutArray{
                mutArray[i]=make([]sync.Mutex, xdim+2)
            }

            sharedWithLocks(leftLong,lowerLat,xBlock,yBlock,&totalPopulation,cutOf,xdim,ydim,array,censusData,mutArray);
            
            grid := make([][]int, ydim+3)
            for i := range grid {
                grid[i] = make([]int, xdim+3)
               
                if i<ydim+2{
                fmt.Println(array[i])}
            }

             for y:=ydim+1;y>0;y--{         //Second step
                    for x:=1;x<xdim+2;x++{
                    grid[y][x] = array[y][x]+grid[y][x-1]+grid[y+1][x]-grid[y+1][x-1]
                }
            }
            population= grid[south][east] - grid[south][west-1] -grid[north+1][east] + grid[north+1][west-1]

            // YOUR QUERY CODE FOR PART 5
        case "-v6":

            array := make([][]int, ydim+2)
            for i := range array {
                array[i] = make([]int, xdim+2)
            }
            fmt.Println("length: ",len(array))

            var mut sync.Mutex;

            array=ver4Parallel(leftLong,lowerLat,xBlock,yBlock,&totalPopulation, cutOf,xdim,ydim,array,censusData,&mut)

            grid := make([][]int, ydim+3)
            for i := range grid {
                grid[i] = make([]int, xdim+3)
               
            }

             for y:=ydim+1;y>0;y--{         //Second step
                    for x:=1;x<xdim+2;x++{
                    grid[y][x] = array[y][x];
                }
            }


            done := make(chan int)

            go fullSum(grid, done);

            <-done
            

            fmt.Println("Rows Done!!!")
            for y:= ydim+1;y>0;y--{
                fmt.Println(grid[y])
            }

            population= grid[south][east] - grid[south][west-1] -grid[north+1][east] + grid[north+1][west-1]
            // YOUR QUERY CODE FOR PART 6
        }
        percentage=(float64(population) /float64(totalPopulation))* 100;
        fmt.Printf("%v %.2f%%\n", population, percentage)
        fmt.Println("Total: ",totalPopulation,percentage)
    }
}
func fullSum(grid [][]int, parent chan int){

    if(len(grid)>1){
        mid := len(grid)/2
        left := make(chan int)
        right := make(chan int)

         go fullSum(grid[:mid],left)
         go fullSum(grid[mid:], right)

         parent<- <-left + <-right
    }else if (len(grid)==1){
    ch := make(chan int)
    go PrefixSum(grid[0] ,ch)
    <-ch
    ch<-0
    <-ch

    parent<- 0
    }
}



func pSum(row []int, done chan int, rowNum int){
    ch := make(chan int)
    go PrefixSum(row, ch)
    <-ch
    ch<-0
    <-ch
    done<- rowNum
}

func pSum2(row []int, done chan int, rowNum int, colCh chan []int){
    ch := make(chan int)
    go PrefixSum(row, ch)
    <-ch
    ch<-0 
    <-ch
    // fmt.Println(row)
    done<- rowNum
    colCh <- row    // passing row over channel
}

func PrefixSum(data []int, parent chan int) {
    if len(data) > 1 {
        mid := len(data)/2
        left := make(chan int)
        right := make(chan int)

        go PrefixSum(data[:mid], left)
        go PrefixSum(data[mid:], right)

        leftSum := <-left
        // fmt.Println("from left ", leftSum)
        parent<- <-right + leftSum

        fromLeft := <-parent
        // fmt.Println("Left from parent ", fromLeft)
        left<- fromLeft
        right<- fromLeft + leftSum

        parent<- <-left + <-right
    } else {
        // fmt.Println(data[0], " passed to parent")
        parent<- data[0]
        data[0] += <-parent
        // fmt.Println("updated ", data[0])
        parent<- 0
    }
}

func sharedWithLocks(leftLong,lowerLat,xBlock,yBlock float64, totalPopulation *int,  cutOf,xdim,ydim int, array [][]int,data []CensusGroup, mutArray [][]sync.Mutex) {

 if(len(data)>cutOf){
        mid:=len(data)/2
        firstHalf := make(chan bool)

            go func(){
               sharedWithLocks(leftLong,lowerLat,xBlock,yBlock,totalPopulation,cutOf,xdim,ydim,array,data[:mid],mutArray)
               firstHalf<- true
            }()
        sharedWithLocks(leftLong,lowerLat,xBlock,yBlock,totalPopulation,cutOf,xdim,ydim,array,data[mid:],mutArray)
        <-firstHalf
        close(firstHalf)

    }else{
        var tPop int;

        for _, dataValue := range data{
            tPop+=dataValue.population
            // fmt.Println(*totalPopulation)
                xPosit,yPosit := func (dataValue CensusGroup) (float64,float64){
                x:=((dataValue.longitude-leftLong) / xBlock) + 1   
                y:=((dataValue.latitude - lowerLat)/ yBlock) + 1
                return x, y;
                }(dataValue)

                mutArray[int(yPosit)][int(xPosit)].Lock()

                array[int(yPosit)][int(xPosit)] += dataValue.population
                
                mutArray[int(yPosit)][int(xPosit)].Unlock()

            }
         // var mut sync.Mutex;   
         // mut.Lock()
        *totalPopulation+= tPop;
        // mut.Unlock()
        // fmt.Println("Going out")
    }
}

func sumParallel(finalArray,firstArray,secondArray []int, fetched chan bool,xdim int){
    for x:=0;x<xdim+2;x++{
    finalArray[x]=firstArray[x]+secondArray[x]; 
       // array[y][x]=firstArray[y][x]+ secondArray[y][x]
    }
    fetched<- true
}

func ver4Parallel(leftLong,lowerLat,xBlock,yBlock float64, totalPopulation *int,  cutOf,xdim,ydim int, array [][]int ,data []CensusGroup, mut *sync.Mutex) ([][]int) {
    
    if(len(data)>cutOf){
        mid:=len(data)/2
        firstHalf := make(chan bool)

         firstArray := make([][]int, ydim+2)
         secondArray := make([][]int, ydim+2)
         finalArray := make([][]int, ydim+2)
         for i := range finalArray {
                finalArray[i] = make([]int, xdim+2)
            }

        go func(){
               // fmt.Println("Total population b4: ",*totalPopulation)
               firstArray=ver4Parallel(leftLong,lowerLat,xBlock,yBlock,totalPopulation,cutOf,xdim,ydim,array,data[:mid],mut)
               firstHalf<- true
            }()           
        secondArray=ver4Parallel(leftLong,lowerLat,xBlock,yBlock,totalPopulation,cutOf,xdim,ydim,array,data[mid:],mut)
        <-firstHalf

        mut.Lock()

        fetched := make(chan bool);
        for y:=0;y<ydim+2;y++{
            go sumParallel(finalArray[y],firstArray[y],secondArray[y],fetched,xdim)
        }
        for y:=0;y<ydim+2;y++{
        <-fetched
        } 

        fmt.Println("\n \n")

        mut.Unlock()
        close(firstHalf)
        // fmt.Println("Total population out after: ",*totalPopulation)

            return finalArray;
    }else{
        var tPop int;
        // var cPop int;
        myArray := make([][]int, ydim+2)
        for i := range array {
            myArray[i] = make([]int, xdim+2)
        }

        for _, dataValue := range data{
            tPop+=dataValue.population
                xPosit,yPosit := func (dataValue CensusGroup) (float64,float64){
                x:=((dataValue.longitude-leftLong) / xBlock) + 1   
                y:=((dataValue.latitude - lowerLat)/ yBlock) + 1
                return x, y;
                }(dataValue)

                myArray[int(yPosit)][int(xPosit)]+=dataValue.population
            }
         // var mut sync.Mutex;   

         mut.Lock()
        *totalPopulation+= tPop;
        mut.Unlock()
        return myArray
    }
}


func simpleParallelQuery(leftLong,lowerLat float64, south,east,north,west,population,totalPopulation *int,cutOf int,xBlock,yBlock float64,data []CensusGroup) {
    
    if(len(data)>cutOf){
        mid:=len(data)/2
        firstHalf := make(chan bool)
        go func(){
               // fmt.Println("Total population b4: ",*totalPopulation)
               simpleParallelQuery(leftLong,lowerLat,south,east,north,west,population,totalPopulation,cutOf,xBlock,yBlock,data[:mid])
               firstHalf<- true
            }()
           
        simpleParallelQuery(leftLong,lowerLat,south,east,north,west,population,totalPopulation,cutOf,xBlock,yBlock,data[mid:])
        <-firstHalf
        close(firstHalf)
        // fmt.Println("Total population out after: ",*totalPopulation)           

    }else{
        // fmt.Println("Came in:", *totalPopulation )
        var tPop int;
        var cPop int;

        for _, dataValue := range data{
            tPop+=dataValue.population
            // fmt.Println(*totalPopulation)
                xPosit,yPosit := func (dataValue CensusGroup) (float64,float64){
                x:=((dataValue.longitude-leftLong) / xBlock) + 1   
                y:=((dataValue.latitude - lowerLat)/ yBlock) + 1
                return x, y;
                }(dataValue)

            cPop+=blockCheck(south,east,north,west,xPosit,yPosit,dataValue)

            }

        *population+=cPop;
        *totalPopulation+= tPop;
    }

}

func findCorners(leftLong,rightLong,lowerLat,upperLat float64, data CensusGroup) (float64,float64,float64,float64){
            if(data.longitude < leftLong){
                leftLong= data.longitude;
            }else if (data.longitude > rightLong){
                rightLong = data.longitude;
            }else if data.latitude < lowerLat {
                lowerLat= data.latitude;
            }else if data.latitude > upperLat{
                upperLat = data.latitude;
            }
return leftLong,rightLong,lowerLat,upperLat
}

func corners(leftLong,rightLong,lowerLat,upperLat *float64, data CensusGroup){
            
            if(data.longitude < *leftLong){
                *leftLong= data.longitude;
            }else if (data.longitude > *rightLong){
                *rightLong = data.longitude;
            }else if data.latitude < *lowerLat {
                *lowerLat= data.latitude;
            }else if data.latitude > *upperLat{
                *upperLat = data.latitude;
            }
}

func simpleParallelCorners( leftLong, rightLong, lowerLat, upperLat *float64, cutOf int, data []CensusGroup){
    if(len(data)>cutOf){
        mid:=len(data)/2
        halfDone := make(chan bool)
        go func(){
               simpleParallelCorners(leftLong,rightLong,lowerLat,upperLat,cutOf,data[:mid])
               halfDone<- true
            }()
        simpleParallelCorners(leftLong,rightLong,lowerLat,upperLat,cutOf,data[mid:])
        <-halfDone

    }else{   //Base case
        for _, value := range data{
    corners(leftLong,rightLong,lowerLat,upperLat,value)
        }
    }

}

func blockCheck(south,east,north,west *int, xPosit, yPosit float64, data CensusGroup) (int){    //Corrosponding to finding against pass by reference value

    var population int;

     if (xPosit > float64(*west)) && (xPosit <= float64(*east)+1) &&  yPosit > float64(*south) && (yPosit <= float64(*north)+1){
                    population+=data.population;
                    return population
                }else if *west==*east && xPosit==float64(*west) && yPosit > float64(*south) && yPosit <= float64(*north)+1 {
                    population+=data.population;   //Lying on a longitude line 
                    return population
                }else if *south==*north && yPosit==float64(*south) && xPosit > float64(*west) && xPosit <= float64(*east)+1{
                    population+=data.population; //lying on a latitude line
                    return population
                }else if *west==*east && xPosit==float64(*west) && *south==*north && yPosit==float64(*south){
                    population+=data.population;  //At aprticular dot
                    return population
                }else if(*west==1 && xPosit==1) && (xPosit <= float64(*east)+1) && yPosit > float64(*south) && (yPosit <= float64(*north)+1){
                    population+=data.population;  //Having extreme left line as well
                    return population
                }else if(*south==1 && yPosit==1) && (yPosit <= float64(*north)+1) && (xPosit > float64(*west)) && (xPosit <= float64(*east)+1){
                    population+=data.population;  //Having extreme lower line as well
                    return population
                }else if(*south==1 && xPosit==1) && (*west==1 && xPosit ==1) && (xPosit <= float64(*east)+1) && (yPosit <= float64(*north)+1){
                    population+=data.population;  //Having extreme lower  and lower line as well
                    return population
                }
    return population
}

func checkBlock(south,east,north,west, population int, xPosit, yPosit float64, data CensusGroup) (int){
     if (xPosit > float64(west)) && (xPosit <= float64(east)+1) &&  yPosit > float64(south) && (yPosit <= float64(north)+1){
                    population+=data.population;
                    return population;
                }else if west==east && xPosit==float64(west) && yPosit > float64(south) && yPosit <= float64(north)+1 {
                    population+=data.population;   //Lying on a longitude line 
                    // fmt.Println("2nd")
                    return population;
                }else if south==north && yPosit==float64(south) && xPosit > float64(west) && xPosit <= float64(east)+1{
                    population+=data.population; //lying on a latitude line
                    // fmt.Println("3rd")
                    return population;
                }else if west==east && xPosit==float64(west) && south==north && yPosit==float64(south){
                    population+=data.population;  //At aprticular dot
                    // fmt.Println("4th")
                    return population;
                }else if(west==1 && xPosit==1) && (xPosit <= float64(east)+1) && yPosit > float64(south) && (yPosit <= float64(north)+1){
                    population+=data.population;  //Having extreme left line as well
                    return population;
                }else if(south==1 && yPosit==1) && (yPosit <= float64(north)+1) && (xPosit > float64(west)) && (xPosit <= float64(east)+1){
                    population+=data.population;  //Having extreme lower line as well
                    return population;
                }else if(south==1 && xPosit==1) && (west==1 && xPosit ==1) && (xPosit <= float64(east)+1) && (yPosit <= float64(north)+1){
                    population+=data.population;  //Having extreme lower  and lower line as well
                    return population;
                }
                
    return population
}

