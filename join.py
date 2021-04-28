import sys
import os
import heapq as hq
import time

total_tuples = 0

#--------------------------------------------------------------------------------------------------------#

def sort_chunk(new_chunk, order, num, f):
    if f=="R":
        new_chunk.sort(key=lambda x:x[1])
    else:
        new_chunk.sort(key=lambda x:x[0])
    
    #print("--> Chunk " + str(num+1) + " sorted")
    chunkName = "S_chunk_" + str(num+1) + ".txt"

    if f=="R":
        chunkName = "R_chunk_" + str(num+1) + ".txt"

    file = open(chunkName,'w') 
    for row in new_chunk:
        data = ""
        for token in row:
            data+=token+" "
        data = data[:-1]
        file.write(data)
        file.write("\n")
    file.close()

#--------------------------------------------------------------------------------------------------------#

def write_chunk(new_chunk, order, num, f):
    #print("--> Chunk " + str(num+1) + " sorted")
    chunkName = "S_chunk_" + str(num+1) + ".txt"

    if f=="R":
        chunkName = "R_chunk_" + str(num+1) + ".txt"

    file = open(chunkName,'w') 
    for row in new_chunk:
        data = ""
        for token in row:
            data+=token+" "
        data = data[:-1]
        file.write(data)
        file.write("\n")
    file.close()

#--------------------------------------------------------------------------------------------------------#

def create_sorted_chunks(Num_of_rows_in_one_chunk, order, ipfilename, f):
    global total_tuples
    chunk = []
    num = 0
    ipfile = open(ipfilename, 'r+')
    while True:
        line = ipfile.readline()
        if not line:
            break
        row = line.strip("\n").split(" ")

        total_tuples+=1
        chunk.append(row)
        if(len(chunk)==Num_of_rows_in_one_chunk):
            sort_chunk(chunk, order, num, f)
            num+=1
            chunk = []
            
    if(len(chunk)>0):
        sort_chunk(chunk, order, num, f)
        num+=1
        chunk = []
    
    return num

#--------------------------------------------------------------------------------------------------------#

def create_chunks(Num_of_rows_in_one_chunk, order, ipfilename, f):
    global total_tuples
    chunk = []
    num = 0
    ipfile = open(ipfilename, 'r+')
    while True:
        line = ipfile.readline()
        if not line:
            break
        row = line.strip("\n").split(" ")

        total_tuples+=1
        chunk.append(row)
        if(len(chunk)==Num_of_rows_in_one_chunk):
            sort_chunk(chunk, order, num, f)
            num+=1
            chunk = []
            
    if(len(chunk)>0):
        sort_chunk(chunk, order, num, f)
        num+=1
        chunk = []

#--------------------------------------------------------------------------------------------------------#

def readFile(filename):
    li = []
    f1 = open(filename, "r")
    data = ""
    while(True):
        data = f1.readline()
        if(data != ""):
            data = data.strip("\n").split(" ")
            li.append(data)
        else:
            break
    f1.close()
    return li

#--------------------------------------------------------------------------------------------------------#

def getnext(Num_of_chunks_R, Num_of_chunks_S):
    f = open("sort_merge_join.txt","w")

    itr1R = 1
    itr1S = 1
    itr2R = 0
    itr2S = 0

    mark = [1,0]
    while(itr1R <= Num_of_chunks_R and itr1S <= Num_of_chunks_S):

        fname_R = "R_chunk_" + str(itr1R)+".txt"
        fname_S = "S_chunk_" + str(itr1S)+".txt"
        
        listR = readFile(fname_R)
        listS = readFile(fname_S)
 
        while(itr2R<len(listR) and itr2S<len(listS)):

            left = listR[itr2R][1]
            right = listS[itr2S][0]
            
            if(left != listS[0][0] and mark == [1,0]):
                                           
                while left < right and itr2R<len(listR) and itr2S<len(listS):
                    itr2R += 1
                    if itr2R < len(listR): 
                        left = listR[itr2R][1]

                while left > right and itr2R<len(listR) and itr2S<len(listS):
                    itr2S += 1
                    if itr2S < len(listS):
                        right = listS[itr2S][0]

                if itr2R == len(listR):
                    break
                elif itr2S == len(listS):
                    break    

                mark = [itr1S, itr2S]

            if(left == right):
                data = ""
                data += listR[itr2R][0] + " "
                data += listR[itr2R][1] + " " 
                data += listS[itr2S][1] + "\n"
                f.write(data)
                itr2S += 1
                if itr2S < len(listS):
                    right = listS[itr2S][0]
            else:
                itr1S = mark[0]
                itr2S = mark[1]
                itr2R += 1
                mark = [1,0]
                fname = "S_chunk_"+str(itr1S)+".txt"
                listS = readFile(fname)
 
        if(itr2S == len(listS)):
            itr1S += 1
            itr2S = 0

        if(itr1S-1 == Num_of_chunks_S):
            itr1S = mark[0]
            itr2S = mark[1]
            itr2R += 1
            mark = [1,0]
            fname = "S_chunk_"+str(itr1S)+".txt"
            listS = readFile(fname)

        if(itr2R == len(listR)):
            itr1R += 1
            itr2R = 0
 
#--------------------------------------------------------------------------------------------------------#

def merge_sorted_chunks(Num_of_chunks, opfilename):
    ci = 0
    if opfilename=="sortedR":
        ci = 1

    opfile = open(opfilename, 'w+')

    chunks_list = []
    datalist = []
    heap = []
    tuple_count = 0

    #print("\n\n--> Final merge Initialising")

    for i in range(Num_of_chunks):
        name = "S_chunk_"+str(i+1)+".txt"
        if opfilename=="sortedR":
            name = "R_chunk_"+str(i+1)+".txt"

        chunkptr = open(name, 'r+')
        chunks_list.append(chunkptr)

    for i in range(Num_of_chunks):
        lineptr = chunks_list[i].readline()
        row = lineptr.strip("\n").split(" ")
    
        datalist.append(row)
        key = row[ci]
        heap.append((key,i))        

        hq.heapify(heap)

    while len(heap)>0:
        tupl = ()
        tupl = hq.heappop(heap)

        index = tupl[1]
        data = datalist[index]
        if data[0]!='':
            data1 = ""
            for tkn in data:
                data1+= tkn + " "
            data1 = data1[:-1]
            
            if tuple_count<total_tuples-1:
                data1 += "\n"
            tuple_count+=1
            opfile.write(data1)

            lineptr = chunks_list[index].readline()
            row = lineptr.strip("\n").split(" ")

            if row[0]=='':
                continue

            datalist[index] = row
            key = row[ci]
            hq.heappush(heap, (key, index))

    opfile.close()

#--------------------------------------------------------------------------------------------------------#

def delete_temp_chunks(Num_of_chunks, f):
    for i in range(Num_of_chunks):
        name = "R_chunk_"+str(i+1)+".txt"
        if f=="S":
            name = "S_chunk_"+str(i+1)+".txt"
        os.remove(name)

#--------------------------------------------------------------------------------------------------------#

def rolling_hash(M, s):
    P = 31
    sum = 0
    power = 1
    for i in range(len(s)):
        power = pow(P, i) % (M-1)
        sum = (sum + (ord(s[i]) * power)) % (M-1)
    return sum

#--------------------------------------------------------------------------------------------------------#

def write_in_file(No_of_chunks, chunk_buffer, chunkName, hash):
    file = open(chunkName,'a') 

    for row in chunk_buffer[hash]:
        data = ""
        for token in row:
            data+=token+" "
        data = data[:-1]
        file.write(data)
        file.write("\n")

    file.close()

#--------------------------------------------------------------------------------------------------------#

def create_hashed_chunks(M, ipfilename, sfilename):
    No_of_chunks = M-1
    chunk_buffer = []

    for i in range(No_of_chunks):
        temp = []
        chunk_buffer.append(temp)
    
    ipfile = open(ipfilename, 'r+')
    nmm = 0
    while True:
        line = ipfile.readline()
        if not line:
            break
        row = line.strip("\n").split(" ")

        s = row[1]
        #print("Finding rolling hash for : ", s)
        hash = rolling_hash(M, s)
        #print("HASH : ", hash)
        chunk_buffer[hash].append(row)
        #print(chunk_buffer)
        if len(chunk_buffer[hash]) == 100:
            chunkName = "hashed_chunk_" + str (hash+1) + ".txt"
            write_in_file(No_of_chunks, chunk_buffer, chunkName, hash)
            empty = []
            chunk_buffer[hash] = empty
        nmm+=1
    
    for i in range(No_of_chunks):
        if len(chunk_buffer[i])>0:
            chunkName = "hashed_chunk_" + str(i+1) + ".txt"
            file = open(chunkName,'a') 

            for row in chunk_buffer[i]:
                data = ""
                for token in row:
                    data+=token+" "
                data = data[:-1]
                file.write(data)
                file.write("\n")

            file.close()

    getnext2(M, sfilename)

#--------------------------------------------------------------------------------------------------------#

def getnext2(M, ipfilename):
    f = open("hash_join.txt","w")

    ipfile = open(ipfilename, 'r+')
    nmm = 0
    while True:
        line = ipfile.readline()
        if not line:
            break
        row = line.strip("\n").split(" ")

        s = row[0]
        #print("Finding rolling hash for : ", s)
        hash = rolling_hash(M, s)

        cname = "hashed_chunk_" + str (hash+1) + ".txt"
        Rchunk = open(cname, 'r+')

        while True:
            ln = Rchunk.readline()
            if not ln:
                break
            Rrow = ln.strip("\n").split(" ")

            if s == Rrow[1]:
                data = ""
                data += Rrow[0] + " "
                data += Rrow[1] + " "
                data += row[1] + "\n"
                f.write(data)
                nmm+=1
        Rchunk.close()
    
    #print("\nNUMBER OF ROWS IN HASH JOIN : ", nmm)
    #print()
    f.close()

    for i in range(1, M):
        try:
            os.remove("hashed_chunk_" + str(i) + ".txt")
        except:
            continue

#--------------------------------------------------------------------------------------------------------#

def Open(filePath, f, M, output_filePath, join):
    Num_of_rows_in_one_chunk = M*100
    if join=="sort":
        Num_of_chunks = create_sorted_chunks(Num_of_rows_in_one_chunk, "asc", filePath, f)
        #print("\nSorted sublists created!\n")

        merge_sorted_chunks(Num_of_chunks, output_filePath)
        delete_temp_chunks(Num_of_chunks, f)
        #print("\n\n==> Sorting Completed.\n\n")
        
        path = "sortedR"
        if f == "S":
            path = "sortedS"

        create_chunks(Num_of_rows_in_one_chunk, "asc", path, f)

        return Num_of_chunks

#--------------------------------------------------------------------------------------------------------#

def Open2(filePath, f, M, output_filePath, join, sfilename):
    create_hashed_chunks(M, filePath, sfilename)

#--------------------------------------------------------------------------------------------------------#

def main():
    st = time.time()
    print()

    n = len(sys.argv)

    if n<5:
        print("Invalid arguments.")
        return

    R_filePath = sys.argv[1]
    S_filePath = sys.argv[2]
    join = sys.argv[3]
    M = int(sys.argv[4])

    if join=="sort":
        NumR = Open(R_filePath, "R", M, "sortedR", join)
        NumS = Open(S_filePath, "S", M, "sortedS", join)
        getnext(NumR, NumS)
        delete_temp_chunks(NumR, "R")
        delete_temp_chunks(NumS, "S")
        os.remove("sortedR")
        os.remove("sortedS")

    else:
        Open2(R_filePath, "R", M, "sortedR", join, S_filePath)

    print("Total execution Time : ", end = " ")
    print(time.time()-st)
    print()
#--------------------------------------------------------------------------------------------------------#

main()