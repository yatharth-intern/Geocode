The scipt contains a code for the problem:

There is a .csv file named "user_pincode_address.csv" which contains the cols 'user', 'pincode', and the 'address' where the user is the unique col here which can be treated as the PK and the users are stored in the sorted order(asc)

There is another file named "user_lat_long.csv" containing the cols 'user', 'lat' and 'long' which contains the users as the Pk but the users are not sorted and the lat and long contains the latitude and the longitude of that user correspondingly,

We have to to extract the lat and long from the pincode of a particular user from the first file. This involves using geocoding library

Then we'll have two set of lat and long. Using the geodesic we need to find the distance between the coordinates in kms

We need to store the data in a new csv file containing 'user' and it's corresponding col 'distance'.

Notes: 
The code should write the data in real time maybe in a batch of 100 to avoid data loss in any kind of error encountered in a loop of such a scale

The code should also deploy time efficient concepts of batch processing maybe and the use of aynchronous processing in it.

There are various error handling lines of code that handles erros such as geocoding erros and timeout errors and values errors.

The code is missing out on speed still (maybe the concept of LRU caching can speed up the process). Still needed to be tried out.
