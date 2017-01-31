#1. Find the total number of records, which will be analyzed
#
#2. Find the total number of unique users
#
#3. Find the total number of unique movies
#
#4. Find the top-3 average-rating by all users,
#output will be:
#
#      user-1 average-rating-1
#      user-2 average-rating-2
#      user-3 average-rating-3
#
#where average-rating-1  > average-rating-2 > average-rating-3 > all other averages
#
#5. Find the number of users who have rated less than
#11, 31, 61, 101 movies:
#
#for example, the output will be:
#
#      5000 users rated  up to 10 movies
#      4000 users rated  up to 30 movies
#      1200 users rated  up to 60 movies
#      6    users rated  up to 100 movies
#
#
#6. Find all users who have rated the same identical movies
#
#
#





from pyspark import SparkConf, SparkContext

# remove header by filter

conf = SparkConf().setMaster("local").setAppName("Users")
sc = SparkContext(conf = conf)
#userId,movieId,rating,timestamp
rdd1=sc.textFile('file:///c:/sparkcourse/ratings.csv')
rdd=rdd1.filter(lambda x:x.split(',')[0]!='userId')
print "Total no of records",rdd.count()
#unique users
users=rdd.map(lambda x:int(x.split(',')[0]))
userscount=users.distinct().count()
print "Unique users",userscount
#unique movie counts
movies=rdd.map(lambda x:x.split(',')[1])
moviescount=movies.distinct().count()
print "Unique movies",moviescount


movierating=rdd.map(lambda x:(int(x.split(',')[1]),int(x.split(',')[0]),float(x.split(',')[2])))
    
# 4.average rating per user by sum of all rating by one user
userandratings=rdd.map(lambda x:(int(x.split(',')[0]),float(x.split(',')[2])))
mapping1=userandratings.map(lambda x: (x[0],(x[1],1)))
reducing1=mapping1.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
averaging1=reducing1.mapValues(lambda x: float(x[0]/x[1]))
userratingsreverse=averaging1.map(lambda (x,y):(y,x))
sorting=userratingsreverse.sortByKey(False).takeOrdered(3,key=lambda x:-x[0])
print " Top average ratings by users"
for i in sorting:
    print i



#5 users rated less than 11, 31, 61, 101 movies ?
moviesraatedbysusers=users.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y)
filterfor10= moviesraatedbysusers.filter(lambda (x,y):y<=10 ).count()

print "No of people rated upto 10 movies",filterfor10


filterfor20=moviesraatedbysusers.filter(lambda (x,y):y>=11 and y<=20).count()
print "No of people rated from 11 to 20 movies",filterfor20

filterfor30=moviesraatedbysusers.filter(lambda (x,y):y>=21 and y<=30).count()
print "No of people rated from 21 t0 30 movies",filterfor30

filterfor40=moviesraatedbysusers.filter(lambda (x,y):y>=31 and y<=40).count()
print "No of people rated from 31 to 40 movies",filterfor40

filterfor50=moviesraatedbysusers.filter(lambda (x,y):y>=41 and y<=50).count()
print "No of people rated from 41 to 50 movies",filterfor50

filterfor200=moviesraatedbysusers.filter(lambda (x,y):y>=51 and y<=200).count()
print "No of people rated from 51 to 200 movies",filterfor200

#6mapping unique users
moviesandratings=rdd.map(lambda x:(int(x.split(',')[0]),int(x.split(',')[1])))
#for i in moviesandratings.collect():
 #   print i
# grouping movies rated by users
grouping=moviesandratings.groupByKey()
#for i in grouping.collect():
 #   print i

listing=grouping.map(lambda x:(x[0],list(x[1])))

#for i in listing.collect():
#    print i

#making movie list as key and grouping user who rated same movies
reverseforlist=listing.map(lambda (x,y):(tuple(y),x))
#print "1"
#for i in reverseforlist.collect():
#    print i
groupingusers=reverseforlist.groupByKey()
#print "2"
#for i in groupingusers.collect():
#    print i
output=groupingusers.map(lambda x:(x[0],list(x[1])))
output1=output.filter(lambda x: len(x[1])>=2 and len(x[0])>=5)
output2=output1.map(lambda (x,y):(y,x))
print "People who rated same movies"
print "Users list: Movieslist"
for i in output2.collect():
    print i


    
