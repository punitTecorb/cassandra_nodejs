import { NextFunction, Request, Response } from 'express';
import { cassandraClient } from '../cassandra_database';
console.log('ksks')
// create keyspace
const create_keyspaces = async (req: any, res: Response, next: NextFunction) => {
    const createKeyspaceQuery = `
    CREATE KEYSPACE IF NOT EXISTS USER
    WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 3                                               
    };
    `;
    await cassandraClient.execute(createKeyspaceQuery);
    next();
}

// create table
const userTable = async (req: any, res: Response, next: NextFunction) => {
    const createTableQuery = `
    CREATE TABLE IF NOT EXISTS USER.user (
    partition_key_column TEXT,
    id UUID,
    age INT,
    name TEXT,
    email TEXT,
    address MAP<TEXT, TEXT>,
    PRIMARY KEY (partition_key_column,id,age,name,email)
    );
   `;

    await cassandraClient.execute(createTableQuery)
    next();
}

const user_serachTable =  async (req:any,res:Response,next:NextFunction) => {
    const query = `CREATE TABLE IF NOT EXISTS USER.user_search (
        partition_key_column TEXT,
        id UUID,
        age INT,
        name TEXT,
        email TEXT,
        address MAP<TEXT, TEXT>,
        PRIMARY KEY ((partition_key_column), name, id)
      );`    
      await cassandraClient.execute(query)
    next();
}

export default {
    create_keyspaces,
    userTable,
    user_serachTable
} as const
