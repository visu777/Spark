#Friend Recommendations using MapReduce           |
#+==================================================+
#In this assignment, we will use PySpark to read an
#input file and recommendations. Problem is described
#Twitter recommendations using Spark


from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

def mapper1(line):
    words = line.split()
    person=words[0]
    friend=words[1]
    return ((person,"follows"),(friend))

def mapper2(line):
    words = line.split()
    person=words[0]
    friend=words[1]

    return ((friend,"followed by"),(person))    
    
def recos1(y):
#    print "y[0][0]"+ y[0][0]
#    print "y[0][1]"
#    print y[0][1]
#    print "y[1][0]"+y[1][0]
#    print "y[1][1]"
#    print y[1][1]
#    print len(y)
    listing=[]
    if (len(y)==2):
#        print len(y[1])
#        print "hi"
        if(y[0][1]=="follows"):
            leaders=y[0][1]
            followers=y[1][1]        
        else:
            leaders=y[0][1] 
            followers=y[1][1]
       
        
        for i in followers:
            z=(i,leaders)
            listing.append(z)
#    print listing
    return listing
    
def reducing(x):
    listing=[]
    if len(x)==2:
        
        if x[0][0]=="follows":
            y=x[0][1]
            z=x[1]
        else:
            y=x[1][1]
            z=x[0]
        for i in z:
            if i in y:
                z.remove(i)
        listing+=z
  
    elif(len(x)==1):
        if x[0][0]=="follows":
            z=x[0][1]
            
        else:
            listing+=x[0]
    return listing
            
        
    
    
    
    
    
rdd=sc.textFile('file:///c:/sparkcourse/ass3data1.txt')
mapper1output=rdd.map(mapper1)
mapper2output=rdd.map(mapper2)



input1=mapper1output.groupByKey()


input2=mapper2output.groupByKey()

finalinput=input1.union(input2)

listvalues=finalinput.mapValues(lambda x:list(x))



commonkey=listvalues.map(lambda ((x,y),z):(x,(y,z)))


grouping=commonkey.groupByKey()
listing=grouping.mapValues(lambda x:list(x))
print"grouping"
for i in listing.collect():
    print i
recos=listing.flatMap(lambda (x,y):recos1(y))
print"flatmap"
for i in recos.collect():
    print i

followslist=input1.map(lambda ((x,y),z):(x,(y,list(z))))
print"follows list"
for i in followslist.collect():
    print i
union=recos.union(followslist)
print "union"
for i in followslist.collect():
    print i
output=union.groupByKey()
output1=output.mapValues(lambda x:list(x))
print "output1"
for i in output1.collect():
   print i
output2=output1.mapValues(lambda x:(reducing(x)))
print "Final output before filter"
for i in output2.collect():
    print i

print"Final output after filter"
for x in output2.collect():
    
    y=int(x[0])
    if (y== 1 or y==27 or y==31 or y==137 or y==3113):
        print x


#output2.saveAsTextFile("output111.txt")
#finaloutput=output2.filter(lambda x: (x[0]== 1 or x[0]==27 or x[0]==31 or x[0]==137 or x[0]==3113))
#print "count" 
#print finaloutput.count()
#for i in finaloutput.collect():
#    print i