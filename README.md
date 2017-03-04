# Smart-Cab-Allocation-System

It is a data stream processing platform just like Uber and Lyft, which smartly allocates cab to users.

It processes cab’s location updates and user requests in Kafka stream; computes the weighted score for each driver based on distance, rating, gender preference and driver’s earnings for the day.

Finally it outputs user request with a matching driver in a key-value format using Samza.

It also implementes dynamic pricing algorithm based on the number of requests and cab availability in a region during given period of time.
