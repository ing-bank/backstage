/*
 * Copyright 2020 The Backstage Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Config, ConfigReader } from '@backstage/config';
import {
  buildPgDatabaseConfig,
  getPgConnectionConfig,
  parsePgConnectionString,
} from './postgres';

import { PgConnector } from './postgres';
import { KnexConnectionTypeTransformer } from '../types';

describe('postgres', () => {
  const createMockConnection = () => ({
    host: 'acme',
    user: 'foo',
    password: 'bar',
    database: 'foodb',
  });

  const createMockConnectionString = () =>
    'postgresql://foo:bar@acme:5432/foodb';

  const createConfig = (connection: any): Config =>
    new ConfigReader({ client: 'pg', connection });

  describe('buildPgDatabaseConfig', () => {
    it('builds a postgres config', async () => {
      const mockConnection = createMockConnection();

      expect(await buildPgDatabaseConfig(createConfig(mockConnection))).toEqual(
        {
          client: 'pg',
          connection: mockConnection,
          useNullAsDefault: true,
        },
      );
    });

    it('builds a connection string config', async () => {
      const mockConnectionString = createMockConnectionString();

      expect(
        await buildPgDatabaseConfig(createConfig(mockConnectionString)),
      ).toEqual({
        client: 'pg',
        connection: mockConnectionString,
        useNullAsDefault: true,
      });
    });

    it('overrides the database name', async () => {
      const mockConnection = createMockConnection();

      expect(
        await buildPgDatabaseConfig(createConfig(mockConnection), {
          connection: { database: 'other_db' },
        }),
      ).toEqual({
        client: 'pg',
        connection: {
          ...mockConnection,
          database: 'other_db',
        },
        useNullAsDefault: true,
      });
    });

    it('overrides the schema name', async () => {
      const mockConnection = {
        ...createMockConnection(),
        schema: 'schemaName',
      };

      expect(
        await buildPgDatabaseConfig(createConfig(mockConnection), {
          searchPath: ['schemaName'],
        }),
      ).toEqual({
        client: 'pg',
        connection: mockConnection,
        searchPath: ['schemaName'],
        useNullAsDefault: true,
      });
    });

    it('adds additional config settings', async () => {
      const mockConnection = createMockConnection();

      expect(
        await buildPgDatabaseConfig(createConfig(mockConnection), {
          connection: { database: 'other_db' },
          pool: { min: 0, max: 7 },
          debug: true,
        }),
      ).toEqual({
        client: 'pg',
        connection: {
          ...mockConnection,
          database: 'other_db',
        },
        useNullAsDefault: true,
        pool: { min: 0, max: 7 },
        debug: true,
      });
    });

    it('overrides the database from connection string', async () => {
      const mockConnectionString = createMockConnectionString();
      const mockConnection = createMockConnection();

      expect(
        await buildPgDatabaseConfig(createConfig(mockConnectionString), {
          connection: { database: 'other_db' },
        }),
      ).toEqual({
        client: 'pg',
        connection: {
          ...mockConnection,
          port: '5432',
          database: 'other_db',
        },
        useNullAsDefault: true,
      });
    });

    it('connection.type without throwing an error', async () => {
      await expect(
        buildPgDatabaseConfig(
          new ConfigReader({
            client: 'pg',
            connection: {
              type: 'a-type',
            },
          }),
        ),
      ).resolves.toHaveProperty('connection.type', 'a-type');
    });
  });

  describe('getPgConnectionConfig', () => {
    it('returns the connection object back', () => {
      const mockConnection = createMockConnection();
      const config = createConfig(mockConnection);

      expect(getPgConnectionConfig(config)).toEqual(mockConnection);
    });

    it('does not parse the connection string', () => {
      const mockConnection = createMockConnection();
      const config = createConfig(mockConnection);

      expect(getPgConnectionConfig(config, true)).toEqual(mockConnection);
    });

    it('automatically parses the connection string', () => {
      const mockConnection = createMockConnection();
      const mockConnectionString = createMockConnectionString();
      const config = createConfig(mockConnectionString);

      expect(getPgConnectionConfig(config)).toEqual({
        ...mockConnection,
        port: '5432',
      });
    });

    it('parses the connection string', () => {
      const mockConnection = createMockConnection();
      const mockConnectionString = createMockConnectionString();
      const config = createConfig(mockConnectionString);

      expect(getPgConnectionConfig(config, true)).toEqual({
        ...mockConnection,
        port: '5432',
      });
    });
  });

  describe('parsePgConnectionString', () => {
    it('parses a connection string uri', () => {
      expect(
        parsePgConnectionString(
          'postgresql://postgres:pass@foobar:5432/dbname?ssl=true',
        ),
      ).toEqual({
        host: 'foobar',
        user: 'postgres',
        password: 'pass',
        port: '5432',
        database: 'dbname',
        ssl: true,
      });
    });
  });

  describe('pgConnector', () => {
    const rootDbConfig = new ConfigReader({
      client: 'pg',
      connection: {},
    });
    describe('createPgDatabaseClient', () => {
      const pgConnector = new PgConnector(rootDbConfig, '');
      it('creates a postgres knex instance', async () => {
        expect(
          await pgConnector.createPgDatabaseClient(
            createConfig({
              host: 'acme',
              user: 'foo',
              password: 'bar',
              database: 'foodb',
            }),
          ),
        ).toBeTruthy();
      });

      it('attempts to read an ssl cert', async () => {
        await expect(() =>
          pgConnector.createPgDatabaseClient(
            createConfig(
              'postgresql://postgres:pass@localhost:5432/dbname?sslrootcert=/path/to/file',
            ),
          ),
        ).rejects.toThrow(/no such file or directory/);
      });
    });
    describe('createPgDatabaseClient with transformers', () => {
      const configExistingTransformer = new ConfigReader({
        client: 'pg',
        connection: {
          type: 'a-type',
        },
      });
      const configMissingTransformer = new ConfigReader({
        client: 'pg',
        connection: {
          type: 'no-transformer-for-this-type',
        },
      });
      const configDefaultTransformer = new ConfigReader({
        client: 'pg',
        connection: {
          type: 'default',
        },
      });
      const transformers: Record<string, KnexConnectionTypeTransformer> = {
        'a-type': jest.fn().mockImplementation(a => a),
      };
      const typeTransformerMock = transformers['a-type'] as jest.Mock;
      const pgConnector = new PgConnector(rootDbConfig, '', transformers);
      it('calls connection type transformer if connection.type is set', async () => {
        await pgConnector.createPgDatabaseClient(configExistingTransformer);
        expect(typeTransformerMock).toHaveBeenCalledTimes(1);
      });
      it('throws if connection.type has no transformer', async () => {
        await expect(() =>
          pgConnector.createPgDatabaseClient(configMissingTransformer),
        ).rejects.toThrow(/no transformer for type/);
      });
      it('does not throw when type is default', async () => {
        expect(
          async () =>
            await pgConnector.createPgDatabaseClient(configDefaultTransformer),
        ).not.toThrow();
      });
    });
  });
});
