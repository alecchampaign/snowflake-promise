const assert = require('node:assert/strict');
const { Readable } = require('node:stream');
const { afterEach, test } = require('node:test');

const SDK = require('snowflake-sdk');
const GlobalConfig = require('snowflake-sdk/dist/lib/global_config');
const Logger = require('snowflake-sdk/dist/lib/logger');
const RequestUtil = require('snowflake-sdk/dist/lib/http/request_util');
const LargeResultSetService = require('snowflake-sdk/dist/lib/services/large_result_set');
const { Snowflake } = require('../build/src');

const originalConfigure = SDK.configure;
const originalCreateConnection = SDK.createConnection;

afterEach(() => {
  SDK.configure = originalConfigure;
  SDK.createConnection = originalCreateConnection;
  originalConfigure({ logLevel: 'OFF' });
  GlobalConfig.setDisableOCSPChecks(false);
});

function makeNativeStatement(rows = [{ VALUE: 1 }, { VALUE: 2 }]) {
  return {
    cancel: callback => callback(),
    getColumn: () => ({ name: 'VALUE' }),
    getColumns: () => [{ name: 'VALUE' }],
    getNumRows: () => rows.length,
    getNumUpdatedRows: () => 0,
    getQueryId: () => 'query-id',
    getRequestId: () => 'request-id',
    getSessionState: () => ({
      getCurrentDatabase: () => 'DATABASE',
      getCurrentSchema: () => 'SCHEMA',
    }),
    getSqlText: () => 'select value from table',
    getStatementId: () => 'query-id',
    getStatus: () => 'complete',
    streamRows: options => {
      assert.deepEqual(options, { start: 0 });
      return Readable.from(rows);
    },
  };
}

test('should preserve the wrapper API used by Substack with SDK 3.3.0', async () => {
  const rows = [{ VALUE: 1 }, { VALUE: 2 }];
  const nativeStatement = makeNativeStatement(rows);
  const calls = [];
  const nativeConnection = {
    connect: callback => callback(),
    connectAsync: callback => callback(),
    destroy: callback => callback(),
    execute: options => {
      calls.push(options);
      process.nextTick(() => options.complete(undefined, nativeStatement, rows));
      return nativeStatement;
    },
    getId: () => 'connection-id',
    isUp: () => true,
  };

  SDK.createConnection = () => nativeConnection;
  SDK.configure = options => calls.push(options);

  const sqlLogs = [];
  const snowflake = new Snowflake(
    { account: 'account', username: 'username' },
    { logLevel: 'debug', logSql: message => sqlLogs.push(message) },
    { disableOCSPChecks: true }
  );

  assert.equal(snowflake.id, 'connection-id');
  assert.equal(snowflake.sdk_connection.isUp(), true);
  assert.deepEqual(calls.slice(0, 2), [{ logLevel: 'DEBUG' }, { disableOCSPChecks: true }]);

  await snowflake.connect();
  await snowflake.connectAsync();

  const statement = snowflake.createStatement({
    sqlText: 'select value from table',
    binds: [1],
    streamResult: true,
  });
  const execution = statement.execute();
  const streamedRows = [];
  for await (const row of statement.streamRows({ start: 0 })) {
    streamedRows.push(row);
  }
  await execution;

  assert.deepEqual(streamedRows, rows);
  assert.deepEqual(await statement.getRows(), rows);
  assert.equal(statement.getStatementId(), 'query-id');
  assert.equal(statement.getRequestId(), 'request-id');
  assert.equal(statement.getStatus(), 'complete');
  assert.equal(statement.getNumRows(), 2);
  assert.equal(statement.getNumUpdatedRows(), 0);
  assert.deepEqual(statement.getColumns(), [{ name: 'VALUE' }]);
  assert.deepEqual(statement.getColumn('VALUE'), { name: 'VALUE' });
  assert.match(sqlLogs[0], /select value from table/);
  assert.match(sqlLogs[0], /with binds: \[1\]/);

  await statement.cancel();
  await snowflake.destroy();
});

test('should pass disableOCSPChecks through to SDK 3.3.0', () => {
  SDK.createConnection = () => ({ getId: () => 'connection-id' });

  new Snowflake(
    { account: 'account', username: 'username' },
    {},
    { disableOCSPChecks: true }
  );

  assert.equal(GlobalConfig.isOCSPChecksDisabled(), true);
});

test('should return one ordered result when SDK 3.3.0 retries an EPIPE chunk download', async () => {
  const expectedRows = [{ VALUE: 1 }, { VALUE: 2 }];
  const expectedBody = Buffer.from(JSON.stringify(expectedRows));
  let requestCount = 0;
  let executionCount = 0;
  const httpClient = {
    request: options => {
      requestCount += 1;
      if (requestCount === 1) {
        const error = new Error('write EPIPE');
        error.code = 'EPIPE';
        options.callback(error);
        return;
      }

      options.callback(
        undefined,
        {
          statusCode: 200,
          headers: { 'content-type': 'application/octet-stream' },
          getResponseHeader: () => 'application/octet-stream',
        },
        expectedBody
      );
    },
  };
  const connectionConfig = {
    getRetryLargeResultSetMaxNumRetries: () => 1,
    getRetryLargeResultSetMaxSleepTime: () => 0,
  };
  const service = new LargeResultSetService(connectionConfig, httpClient);
  const nativeStatement = makeNativeStatement(expectedRows);
  nativeStatement.streamRows = () => {
    const stream = new Readable({ objectMode: true, read() {} });
    service.getObject({
      url: 'https://example.s3.amazonaws.com/chunk',
      callback: (error, result) => {
        if (error) {
          stream.destroy(error);
          return;
        }
        for (const row of JSON.parse(result.toString('utf8'))) {
          stream.push(row);
        }
        stream.push(null);
      },
    });
    return stream;
  };
  SDK.createConnection = () => ({
    execute: options => {
      executionCount += 1;
      process.nextTick(() => options.complete(undefined, nativeStatement));
      return nativeStatement;
    },
  });

  const snowflake = new Snowflake({ account: 'account', username: 'username' });
  const statement = snowflake.createStatement({
    sqlText: 'select value from table',
    streamResult: true,
  });
  const execution = statement.execute();
  const actualRows = [];
  for await (const row of statement.streamRows()) {
    actualRows.push(row);
  }
  await execution;

  assert.equal(requestCount, 2);
  assert.equal(executionCount, 1);
  assert.deepEqual(actualRows, expectedRows);
});

test('should not retry canceled large-result downloads with SDK 3.3.0', async () => {
  let requestCount = 0;
  const httpClient = {
    request: options => {
      requestCount += 1;
      const error = new Error('canceled');
      error.code = 'ERR_CANCELED';
      options.callback(error);
    },
  };
  const connectionConfig = {
    getRetryLargeResultSetMaxNumRetries: () => 10,
    getRetryLargeResultSetMaxSleepTime: () => 0,
  };
  const service = new LargeResultSetService(connectionConfig, httpClient);

  await assert.rejects(
    new Promise((resolve, reject) => {
      service.getObject({
        url: 'https://example.s3.amazonaws.com/chunk',
        callback: error => error ? reject(error) : resolve(),
      });
    }),
    error => error.name === 'NetworkError' && error.cause.code === 'ERR_CANCELED'
  );
  assert.equal(requestCount, 1);
});

test('should omit presigned query strings and mask credentials in logs with SDK 3.3.0', () => {
  const credential = 'AKIA_TEST_CREDENTIAL';
  const securityToken = 'SECURITY_TOKEN_VALUE';
  const messages = [];
  const customLogger = Object.fromEntries(
    ['error', 'warn', 'info', 'debug', 'trace'].map(level => [level, message => messages.push(message)])
  );
  originalConfigure({ logLevel: 'DEBUG', customLogger });

  const presignedUrl =
    'https://example.s3.amazonaws.com/chunk?X-Amz-Credential=' + credential +
    '&X-Amz-Security-Token=' + securityToken + '&X-Amz-Signature=signaturevalue1234';
  Logger.getInstance().debug('Cloud storage request %s', RequestUtil.describeURL(presignedUrl));
  Logger.getInstance().debug(
    'Fallback masking X-Amz-Credential=%s&X-Amz-Security-Token=%s',
    credential,
    securityToken
  );

  const output = messages.join('\n');
  assert.match(output, /example\.s3\.amazonaws\.com/);
  assert.match(output, /path=\/chunk/);
  assert.doesNotMatch(output, /\?/);
  assert.doesNotMatch(output, new RegExp(credential));
  assert.doesNotMatch(output, new RegExp(securityToken));
  assert.match(output, /X-Amz-Credential=\*\*\*\*/);
  assert.match(output, /X-Amz-Security-Token=\*\*\*\*/);
});
