import express, { json, query } from 'express'
const app = express();
import cassandra from 'cassandra-driver'
app.use(express.json())
import { cassandraClient } from './cassandra_database';
import userModel from './models/user'
import { orderTable } from './models/order';

//add user
app.post('/add', userModel.userTable, async (req, res) => {
    try {
        const userAddresses = {
            home: '123 Main St',
            work: '456 Business Ave',
            street: "Noida",
            lat: "72.9292",
            long: "28.9928"
        };
        const insertQuery = `
        INSERT INTO USER.user (id, name, age, email,address,partition_key_column)
        VALUES (?, ?, ?, ?, ? , ?);
      `;
        const params = [cassandra.types.Uuid.random(), req.body.name, req.body.age, req.body.email, userAddresses, "partition_key_column_user"];
        const data = await cassandraClient.execute(insertQuery, params, { prepare: true })
        return res.json({ message: 'success', data: data })
    } catch (err: any) {
        return res.json({ error: err })
    }
})

//edit user
app.post('/edit', async (req, res) => {
    try {
        const userAddresses = {
            home: '123 Main St',
            work: '456 Business Ave',
            street: "Noida"
        };
        const id = req.body.userId
        const query = 'UPDATE USER.user SET name = ?,email = ?, addresses = ? WHERE partition_key_column = ? AND  id = ?'
        const updateParams = [req.body.name, req.body.email, userAddresses, "partition_key_column_user", id];
        const result = await cassandraClient.execute(query, updateParams, { prepare: true });
        return res.json({ message: 'success', data: result });
    } catch (err: any) {
        console.log(err, "slsls")
        return res.json({ error: err })
    }
})

//delete user
app.post('/delete', async (req, res) => {
    try {
        const query = 'DELETE FROM USER.user WHERE id = ?'
        const params = [req.body.id];
        const data = await cassandraClient.execute(query, params);
        return res.json({ message: 'success', result: data })
    } catch (err) {
        return res.json({ error: err })
    }
})
//details user
app.post('/details', async (req, res) => {
    try {
        const id = req.body.userId
        // const query = 'SELECT name, age,email,id FROM USER.user WHERE partition_key_column = ? AND age = ? ALLOW FILTERING'
        const query = 'SELECT name, age,email,id,addresses FROM USER.user WHERE partition_key_column = ? AND id = ? '
        const params = ["partition_key_column_user", id];
        const result = await cassandraClient.execute(query, params, { prepare: true });
        return res.json({ message: 'success', data: result.rows })
    } catch (err) {
        return res.json({ error: err })
    }
})
//userlist sorting by age
app.get('/list', async (req, res) => {
    try {
        const pageSize = req.body.perPage ? req.body.perPage : 10
        const pageState = req.body.page ? req.body.page : null
        const selectQuery = `SELECT * FROM USER.user_by_age WHERE partition_key_column = ? AND name = ? ORDER BY age ASC ALLOW FILTERING`;
        const countQuery = 'select count(*) from User.user_by_age where partition_key_column = ? AND name =?';
        const selectParams = ['partition_key_column_user', req.body.name];
        const options = { pageState, prepare: true, fetchSize: pageSize };
        const data = await cassandraClient.execute(selectQuery, selectParams, options)
        return res.json({ message: 'success', result: data.rows, nextPage: data.pageState })
    } catch (err) {
        return res.json({ error: err })
    }
})

//userlist sorting by age and name
app.get('/list_name', async (req, res) => {
    try {
        const createMaterializedViewQuery = `
          CREATE MATERIALIZED VIEW IF NOT EXISTS USER.user_by_name AS
          SELECT partition_key_column,id,name, age, email,address
          FROM USER.user
          WHERE partition_key_column IS NOT NULL AND name IS NOT NULL
          AND id IS NOT NULL AND age IS NOT NULL AND email IS NOT NULL
          PRIMARY KEY (partition_key_column,name,age,id,email)
          WITH CLUSTERING ORDER BY (name ASC,age ASC,id DESC,email DESC);
        `;
        await cassandraClient.execute(createMaterializedViewQuery);

        const selectQuery = `SELECT * FROM USER.user_by_name WHERE partition_key_column = ? AND name >= ? AND name < ? ORDER BY age DESC`;
        const selectParams = ["partition_key_column_user",  req.body.name,req.body.name+"\uffff" ];
        const data = await cassandraClient.execute(selectQuery, selectParams, { prepare: true })
        return res.json({ message: 'success', result: data.rows })
    } catch (err) {
        return res.json({ error: err })
    }
})

//Drop or Delete tables
app.get('/drop_tables', async (req, res) => {
    try {
        //DROP MATERIALIZED VIEW
        // const dropold_DROP_MATERIALIZED_VIEW = `DROP MATERIALIZED VIEW IF EXISTS USER.user_by_age`
        //drop table
        const dropOldTableQuery = `
        DROP TABLE IF EXISTS USER.user;
      `;
        await cassandraClient.execute(dropOldTableQuery);
        return res.json({ message: "success" })
    } catch (err) {
        return res.json({ error: err });
    }
})

//pagination 
app.get('/list_pagination', async (req, res) => {
    try {
        const searchTerm = "Aas"
        const pageSize = req.body.perPage ? req.body.perPage : 10
        const pageState = req.body.page ? req.body.page : null
        const query = 'SELECT * FROM USER.user WHERE partition_key_column = ? AND name = ? ';
        const selectParams = ['partition_key_column_user', searchTerm];
        const options = { pageState, prepare: true, fetchSize: pageSize };
        const result = await cassandraClient.execute(query, selectParams, options);
        return res.json({ message: "success", data: result.rows, nextPage: result.pageState });
    } catch (err) {
        return res.json({ error: err });
    }
});

//search
app.get('/search', userModel.user_serachTable, async (req, res) => {
    try {
        const searchTerm = req.body.name
        const selectQuery = `SELECT * FROM USER.user WHERE name LIKE ?`;
        const selectParams = [searchTerm + "%"];
        const data = await cassandraClient.execute(selectQuery, selectParams, { prepare: true });
        return res.json({ message: 'success', result: data.rows })

    } catch (err) {
        console.log(err)
        return res.json({ error: err })
    }
})

//create order
app.post('/addOrder', orderTable, async (req, res) => {
    try {
        const insertQuery = `
        INSERT INTO USER.orderData (id, userId, orderName, amount,partition_key)
        VALUES (?, ?, ?, ?, ?);
      `;
        const userQuery = 'SELECT * FROM USER.user WHERE id = ?'
        const h = await cassandraClient.execute(userQuery, [req.body.userId]);
        console.log(h, "s;lslsls ")
        const params = [cassandra.types.Uuid.random(), req.body.userId, req.body.orderName, req.body.amount, "partition_key1"];
        const userDetails = await cassandraClient.execute()
        await cassandraClient.execute(insertQuery, params, { prepare: true })
            .then((result: any) => {
                return res.json({ message: "success", data: result })
            })
            .catch((err: any) => {
                console.error('Error inserting data', err);
            });
    } catch (err) {
        return res.json({ error: err })
    }
})

app.get('/orderList', async (req, res) => {
    try {
        const createMaterializedViewQuery = `
          CREATE MATERIALIZED VIEW IF NOT EXISTS USER.user_by_id AS
          SELECT partition_key_column,id,name, age, email
          FROM USER.user
          WHERE partition_key_column IS NOT NULL AND name IS NOT NULL
          AND id IS NOT NULL AND age IS NOT NULL
          PRIMARY KEY (partition_key_column, id,name, age)
          WITH CLUSTERING ORDER BY (id ASC,name ASC, age ASC);
        `;
        await cassandraClient.execute(createMaterializedViewQuery);
        var obj: any = {}
        var Array: any = [];
        const query = `SELECT * FROM USER.orderData`;
        const userQuery: any = `SELECT * FROM USER.user_by_id`;
        var result: any = await cassandraClient.execute(query, []);
        if (result.rows.length) {
            for (let i = 0; i <= result.rows.length; i++) {
                const id = result.rows[i]
                var result1 = await cassandraClient.execute(userQuery, []);
                obj = {
                    orderDetails: result.rows[i],
                    userDetails: result1.rows
                }
                Array.push(obj);
            }
        }
        return res.json({ message: "success", data: result1.rows })
    } catch (err) {
        console.log(err)
        return res.json({ error: err })
    }
})

app.post('/add_hash', async (req, res) => {
    try {
        const createUsersTableQuery1 = `
        CREATE TABLE IF NOT EXISTS USER.user_address (
          user_id UUID PRIMARY KEY,
          name TEXT,
          email SET<TEXT>,
          addresses MAP<TEXT, TEXT>,
        );
      `;
        await cassandraClient.execute(createUsersTableQuery1);
        const userAddresses = {
            home: '123 Main St',
            work: '456 Business Ave'
        };
        const insert = `INSERT INTO USER.user_address (user_id,name,email,addresses)
      VALUES (?, ?, ?, ?)`;
        const params = [cassandra.types.Uuid.random(), "ashu", ['ashuqwe12@gmail.com', 'ddd3@gmail.com'], userAddresses]
        const h = await cassandraClient.execute(insert, params, { prepare: true });
        return res.json({ message: "success", data: h })
    } catch (err) {
        console.log(err, "slslkskisks")
        return res.json({ error: err });
    }
})
app.listen(3879, () => {
    console.log('Server is running on 3879')
})