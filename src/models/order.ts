import { NextFunction, Request, Response } from 'express';
import { cassandraClient } from '../cassandra_database';
const orderTable = async (req: any, res: Response, next: NextFunction) => {
    const createTableQuery = `
    CREATE TABLE IF NOT EXISTS USER.orderData (
    partition_key TEXT,
    id UUID,
    userId TEXT,
    orderName TEXT,
    amount Int,
    PRIMARY KEY (partition_key,amount,id,userId)
    ) WITH CLUSTERING ORDER BY (amount ASC,id ASC,userId ASC);
   `;
    await cassandraClient.execute(createTableQuery)
    next();
}

export {
    orderTable
}