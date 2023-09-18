import cassandra  from "cassandra-driver";
import express from 'express'
const app = express();
app.use(express.json());
var clientId1 = "Your client Id";
var secret = "Your secret token";
var token = "Your token";

const client = new cassandra.Client({
  cloud: {
    secureConnectBundle: "path",
  },
  credentials: {
    username: clientId1,
    password: secret,
  },
});

client.connect().then(() => {
  console.log("connected")
}).catch((err: any) => {
  console.log('connection failled')
});

//add user
app.post('/add', async (req, res) => {
  try {
    const createKeyspaceQuery = `
    CREATE KEYSPACE IF NOT EXISTS user1
    WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 3                                               
    };
    `;
    await client.execute(createKeyspaceQuery);

    const createTableQuery = `
    CREATE TABLE IF NOT EXISTS user1.user (
    partition_key_column TEXT,
    id UUID,
    name TEXT,
    age INT,
    email TEXT,
    PRIMARY KEY (partition_key_column, id,age)
    );
   `;
    await client.execute(createTableQuery)

    const insertQuery = `
      INSERT INTO user1.user (id, name, age, email,partition_key_column)
      VALUES (?, ?, ?, ?, ?);
    `;

    const params = [cassandra.types.Uuid.random(), req.body.name, req.body.age, req.body.email, "partition_key_column_user"];

    const data = await client.execute(insertQuery, params, { prepare: true })
    return res.json({ message: 'success', data: data })

  } catch (err: any) {
    return res.json({ error: err })
  }
})

//userlist
app.get('/list', async (req, res) => {
  try {
      const selectQuery = `SELECT id, name, age, email
      FROM user1.user`;
      const selectParams = ["partition_key_column_user", `${req.body.search}%`];
      console.log(selectParams)
      const data = await client.execute(selectQuery, [], { prepare: true })
      return res.json({ message: 'success', result: data.rows })
  } catch (err) {
      return res.json({ error: err })
  }
})

app.listen(3879, () => {
  console.log("Server is running on 3879")
})

