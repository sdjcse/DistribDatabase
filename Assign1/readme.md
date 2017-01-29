allFuns.py,Interface.py -> Contains invocation to all tasks in the assignment
crudOperations.py -> Contains all the DB commands and query execution parts
deletePart.py -> Function to invoke deletion of all the partitions(will not delete the ratings table)
loadRatings.py -> Function to invoke load operation into the DB
rangeInsert.py -> Function to invoke insertion into the range partition
rangePartition.py -> Range partition invoker
roundRobinPartition.py -> Round robin partition invoker
rrobinInsert.py -> Function to invoke insertion into the round robin partitioned data

Sample Run: with ratings.dat file

/usr/bin/python2.7 /home/sdj/DDS/Assignments/Assign1/AssignmentTester.py
A database named "test_dds_assgn1" already exists
T: 2017-01-29 14:07:06 Testing LoadingRating()
Inserting successful
0.0354189872742
T: 2017-01-29 14:07:06 Took 0.03609s for "'testloadratings'()"
T: 2017-01-29 14:07:06 Test passed!

T: 2017-01-29 14:07:06 Testing RangePartition()
T: 2017-01-29 14:07:06 Took 0.33178s for "'testrangepartition'()"
T: 2017-01-29 14:07:06 Test passed!

T: 2017-01-29 14:07:06 Testing RoundRobinPartition()
Round Robin Partitions done!
0.379158973694
T: 2017-01-29 14:07:07 Took 0.38525s for "'testroundrobinpartition'()"
T: 2017-01-29 14:07:07 Test passed!

T: 2017-01-29 14:07:07 Testing RoundRobinInsert()
Insert Successful!
0.0157418251038
T: 2017-01-29 14:07:07 Took 0.01692s for "'testroundrobininsert'()"
T: 2017-01-29 14:07:07 Test passed!

T: 2017-01-29 14:07:07 Testing RangeInsert()
Range Insertion successful!
0.00968885421753
T: 2017-01-29 14:07:07 Took 0.01051s for "'testrangeinsert'()"
T: 2017-01-29 14:07:07 Test passed!

Press enter to Delete all tables? 
T: 2017-01-29 14:07:12 Deleting all testing tables using your own function
Deleted all partitions!
0.11514210701

Process finished with exit code 0
