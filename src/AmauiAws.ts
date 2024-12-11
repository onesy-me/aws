import * as AWS_S3 from '@aws-sdk/client-s3';
import express from 'express';
import { Readable } from 'stream';

import is from '@onesy/utils/is';
import merge from '@onesy/utils/merge';
import parse from '@onesy/utils/parse';
import stringify from '@onesy/utils/stringify';
import { OnesyAwsError } from '@onesy/errors';
import OnesyDate from '@onesy/date/OnesyDate';
import duration from '@onesy/date/duration';
import OnesyLog from '@onesy/log';

export interface IConnections {
  s3?: AWS_S3.S3Client;
}

export interface IOptionsS3Add extends AWS_S3.PutObjectCommandInput {
  bucketName?: string;
}

export interface IOptionsS3Get extends AWS_S3.GetObjectCommandInput {
  bucketName?: string;
  type: 'buffer' | 'json' | 'text';
  pure?: boolean;
}

export interface IOptionsS3Remove extends AWS_S3.DeleteObjectCommandInput {
  bucketName?: string;
  pure?: boolean;
}

export interface IOptionsS3RemoveMany extends AWS_S3.DeleteObjectCommandInput {
  bucketName?: string;
  pure?: boolean;
}

export interface IOptionsS3Credentials {
  accessKeyId: string;
  secretAccessKey: string;
}

export interface IOptionsS3 {
  bucketName?: string;
  credentials: IOptionsS3Credentials;
  endpoint: string;
  region?: string;
}

export interface IOptionsConfig {
  region?: string;
  apiVersion?: string;
  signatureVersion?: string;
  s3ForcePathStyle?: boolean;

  [p: string]: any;
}

export interface IOptions {
  s3: IOptionsS3;
  config?: IOptionsConfig;
}

const getData = (stream: Readable) => {
  return new Promise<Buffer>((resolve, reject) => {
    const chunks: Buffer[] = []
    stream.on('data', chunk => chunks.push(chunk))
    stream.once('end', () => resolve(Buffer.concat(chunks)))
    stream.once('error', reject)
  })
};

const optionsDefault = {
  config: {
    apiVersion: '2006-03-01',
    signatureVersion: 'v4',
    s3ForcePathStyle: true
  }
};

export class OnesyAws {
  private options: IOptions;
  private connections_: IConnections = {};
  private amalog: OnesyLog;

  public get connections(): IConnections {
    if (!this.connections_.s3) this.connections_.s3 = new AWS_S3.S3Client({
      region: this.options.config.region,

      forcePathStyle: true,

      ...this.options.s3
    });

    return this.connections_;
  }

  public constructor(options: IOptions) {
    this.options = merge(options, optionsDefault);

    // Get initial connection
    this.connections;

    this.amalog = new OnesyLog({
      arguments: {
        pre: ['AWS', 'S3'],
      },
    });
  }

  public get s3() {
    const thisClass = this;

    return {
      async add(id: string, value_: any, options: Partial<IOptionsS3Add> = {}): Promise<AWS_S3.PutObjectOutput> {
        const connection = thisClass.connections.s3;
        const start = OnesyDate.utc.milliseconds;

        const {
          bucketName,

          ...other
        } = options;

        const Bucket = bucketName || thisClass.options.s3.bucketName;

        let value = value_;

        if (!(is('string', value) || is('buffer', value))) value = stringify(value);

        try {
          const response = await connection.send(
            new AWS_S3.PutObjectCommand({
              Key: String(id),
              Body: Buffer.from(value, 'binary'),
              Bucket,

              ...other
            })
          );

          return thisClass.response(start, bucketName, 'add', response);
        }
        catch (error) {
          thisClass.response(start, bucketName, 'add');

          throw new OnesyAwsError(error);
        }
      },

      async get(id: string, options: Partial<IOptionsS3Get> = { type: 'buffer' }): Promise<AWS_S3.GetObjectOutput | Buffer | string | object> {
        const connection = thisClass.connections.s3;
        const start = OnesyDate.utc.milliseconds;

        const {
          bucketName,
          type,
          pure,
          ...other
        } = options;

        const Bucket = options.bucketName || thisClass.options.s3.bucketName;

        try {
          const response = await connection.send(
            new AWS_S3.GetObjectCommand({
              Key: String(id),
              Bucket,

              ...other
            })
          );

          let value: any = await getData(response.Body as any);

          if (value !== undefined) {
            if (['json', 'text'].indexOf(type) > -1) value = value.toString() as any;

            if (['json'].indexOf(type) > -1) value = parse(value.toString());
          }

          return thisClass.response(start, bucketName, 'get', pure ? response : value);
        }
        catch (error) {
          if (['NoSuchKey'].indexOf(error.code) > -1) return thisClass.response(start, bucketName, 'get');

          thisClass.response(start, bucketName, 'get');

          throw new OnesyAwsError(error);
        }
      },

      async remove(id: string, options: Partial<IOptionsS3Remove> = {}): Promise<AWS_S3.DeleteObjectOutput | boolean> {
        const connection = thisClass.connections.s3;
        const start = OnesyDate.utc.milliseconds;

        const {
          bucketName,
          pure,

          ...other
        } = options;

        const Bucket = bucketName || thisClass.options.s3.bucketName;

        try {
          const response = await connection.send(
            new AWS_S3.DeleteObjectCommand({
              Key: String(id),
              Bucket,

              ...other
            })
          );

          const value = response.DeleteMarker;

          return thisClass.response(start, bucketName, 'remove', pure ? response : value);
        }
        catch (error) {
          if (['NoSuchKey'].indexOf(error.code) > -1) return thisClass.response(start, bucketName, 'remove');

          thisClass.response(start, bucketName, 'remove');

          throw new OnesyAwsError(error);
        }
      },

      async removeMany(ids: string[], options: Partial<IOptionsS3RemoveMany> = {}): Promise<Array<AWS_S3.DeleteObjectOutput | boolean | Error>> {
        const responses = [];
        const start = OnesyDate.utc.milliseconds;

        const bucketName = options.bucketName || thisClass.options.s3.bucketName;

        for (const id of ids) {
          try {
            const response = await thisClass.s3.remove(id, options);

            responses.push(response);
          }
          catch (error) {
            responses.push(error);
          }
        }

        return thisClass.response(start, bucketName, 'removeMany', responses);
      }
    };
  }

  protected response(
    start: number,
    bucketName: string,
    method: string,
    value?: any,
    req?: express.Request
  ): any {
    if (is('number', start)) {
      const arguments_ = [];

      if (bucketName) arguments_.push(`Bucket: ${bucketName}`);
      if (method) arguments_.push(`Method: ${method}`);
      if ((req as any)?.id) arguments_.push(`Request ID: ${(req as any).id}`);

      arguments_.push(`Duration: ${duration(OnesyDate.utc.milliseconds - start, true)}`);

      this.amalog.debug(...arguments_);
    }

    return value;
  }

}

export default OnesyAws;
