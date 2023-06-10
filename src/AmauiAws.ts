import * as AWS_S3 from '@aws-sdk/client-s3';
import express from 'express';

import is from '@amaui/utils/is';
import merge from '@amaui/utils/merge';
import parse from '@amaui/utils/parse';
import stringify from '@amaui/utils/stringify';
import { AmauiAwsError } from '@amaui/errors';
import AmauiDate from '@amaui/date/amaui-date';
import duration from '@amaui/date/duration';
import AmauiLog from '@amaui/log';

export interface IConnections {
  s3?: AWS_S3.S3Client;
}

export interface IOptionsS3Add {
  bucketName?: string;
}

export interface IOptionsS3Get {
  bucketName?: string;
  type: 'buffer' | 'json' | 'text';
  pure?: boolean;
}

export interface IOptionsS3Remove {
  bucketName?: string;
  pure?: boolean;
}

export interface IOptionsS3RemoveMany {
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

const optionsDefault = {
  config: {
    apiVersion: '2006-03-01',
    signatureVersion: 'v4',
    s3ForcePathStyle: true
  }
};

export class AmauiAws {
  private options: IOptions;
  private connections_: IConnections = {};
  private amalog: AmauiLog;

  public get connections(): IConnections {
    if (!this.connections_.s3) this.connections_.s3 = new AWS_S3.S3Client({
      region: this.options.config.region,

      ...this.options.s3
    });

    return this.connections_;
  }

  public constructor(options: IOptions) {
    this.options = merge(options, optionsDefault);

    // Get initial connection
    this.connections;

    this.amalog = new AmauiLog({
      arguments: {
        pre: ['AWS', 'S3'],
      },
    });
  }

  public get s3() {
    const thisClass = this;

    return {
      async add(id: string, value_: any, options: IOptionsS3Add = {}): Promise<AWS_S3.PutObjectOutput> {
        const connection = thisClass.connections.s3;
        const start = AmauiDate.utc.milliseconds;

        const bucketName = options.bucketName || thisClass.options.s3.bucketName;

        let value = value_;

        if (!(is('string', value) || is('buffer', value))) value = stringify(value);

        try {
          const response = await connection.send(
            new AWS_S3.PutObjectCommand({
              Key: String(id),
              Body: Buffer.from(value, 'binary'),
              Bucket: bucketName,
            })
          );

          return thisClass.response(start, bucketName, 'add', response);
        }
        catch (error) {
          thisClass.response(start, bucketName, 'add');

          throw new AmauiAwsError(error);
        }
      },

      async get(id: string, options: IOptionsS3Get = { type: 'buffer' }): Promise<AWS_S3.GetObjectOutput | Buffer | string | object> {
        const connection = thisClass.connections.s3;
        const start = AmauiDate.utc.milliseconds;

        const { type, pure } = options;
        const bucketName = options.bucketName || thisClass.options.s3.bucketName;

        try {
          const response = await connection.send(
            new AWS_S3.GetObjectCommand({
              Key: String(id),
              Bucket: bucketName,
            })
          );

          let value = response.Body;

          if (value !== undefined) {
            if (['json', 'text'].indexOf(type) > -1) value = value.toString() as any;

            if (['json'].indexOf(type) > -1) value = parse(value);
          }

          return thisClass.response(start, bucketName, 'get', pure ? response : value);
        }
        catch (error) {
          if (['NoSuchKey'].indexOf(error.code) > -1) return thisClass.response(start, bucketName, 'get');

          thisClass.response(start, bucketName, 'get');

          throw new AmauiAwsError(error);
        }
      },

      async remove(id: string, options: IOptionsS3Remove = {}): Promise<AWS_S3.DeleteObjectOutput | boolean> {
        const connection = thisClass.connections.s3;
        const start = AmauiDate.utc.milliseconds;

        const { pure } = options;
        const bucketName = options.bucketName || thisClass.options.s3.bucketName;

        try {
          const response = await connection.send(
            new AWS_S3.DeleteObjectCommand({
              Key: String(id),
              Bucket: bucketName,
            })
          );

          const value = response.DeleteMarker;

          return thisClass.response(start, bucketName, 'remove', pure ? response : value);
        }
        catch (error) {
          if (['NoSuchKey'].indexOf(error.code) > -1) return thisClass.response(start, bucketName, 'remove');

          thisClass.response(start, bucketName, 'remove');

          throw new AmauiAwsError(error);
        }
      },

      async removeMany(ids: string[], options: IOptionsS3RemoveMany = {}): Promise<Array<AWS_S3.DeleteObjectOutput | boolean | Error>> {
        const responses = [];
        const start = AmauiDate.utc.milliseconds;

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

      arguments_.push(`Duration: ${duration(AmauiDate.utc.milliseconds - start, true)}`);

      this.amalog.debug(...arguments_);
    }

    return value;
  }

}

export default AmauiAws;
