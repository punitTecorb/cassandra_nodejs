import cassandra from 'cassandra-driver'

// Create and export the Cassandra client instance
export const cassandraClient: any = new cassandra.Client({
    contactPoints: ['127.0.0.1:9045'],
    localDataCenter: 'datacenter1',
    credentials: { username: 'your username', password: 'your password' }
});


// Connect to the Cassandra cluster
cassandraClient.connect()
    .then(() => {
        console.log('Connected to Cassandra');
    })
    .catch((err: any) => {
        console.error('Error connecting to Cassandra', err);
    });