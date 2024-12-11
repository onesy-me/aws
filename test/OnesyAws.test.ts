/* tslint:disable: no-shadowed-variable */
import { assert } from '@onesy/test';

import * as OnesyUtils from '@onesy/utils';
import OnesyLog from '@onesy/log';

import OnesyAws from '../src';

import Config from '../utils/js/config';

const options = {
  s3: {
    bucketName: Config.config.aws.s3.bucketName,

    credentials: {
      accessKeyId: Config.config.aws.s3.accessKeyId,
      secretAccessKey: Config.config.aws.s3.secretAccessKey
    },

    endpoint: Config.config.aws.s3.endpoint,

    region: Config.config.aws.s3.region
  }
};

group('@onesy/aws', () => {

  group('OnesyAws', () => {
    let onesyAws: OnesyAws;
    const addedItems = [];

    pre(() => {
      onesyAws = new OnesyAws(options);

      OnesyLog.options.log.enabled = false;
    });

    post(async () => {
      await onesyAws.s3.removeMany(addedItems.map(item => item.id));

      OnesyLog.options.log.enabled = true;
    });

    group('connections', () => {

      to('s3', () => {
        assert((onesyAws.connections.s3 as any)._clientId).eq(1);
      });

    });

    group('s3', () => {

      group('add', () => {

        to('text', async () => {
          const response = await onesyAws.s3.add('1', 'a');

          assert(response.ETag).exist;

          (response as any).id = '1';

          addedItems.push(response);
        });

        to('object', async () => {
          const response = await onesyAws.s3.add('2', { a: 'a4' });

          assert(response.ETag).exist;

          (response as any).id = '2';

          addedItems.push(response);
        });

        to('buffer', async () => {
          const response = await onesyAws.s3.add('3', Buffer.from('a'));

          assert(response.ETag).exist;

          (response as any).id = '3';

          addedItems.push(response);
        });

      });

      group('get', () => {

        group('options', () => {

          to('pure', async () => {
            const response = await onesyAws.s3.get('1', { type: 'text', pure: true }) as any;
            const response1 = await onesyAws.s3.get('1', { type: 'text', pure: false }) as any;

            assert(response.AcceptRanges).eq('bytes');
            assert(OnesyUtils.is('buffer', response.Body)).eq(true);

            assert(response1).eq('a');
          });

        });

        to('text', async () => {
          const response = await onesyAws.s3.get('1', { type: 'text' });

          assert(response).eq('a');
        });

        to('object', async () => {
          const response = await onesyAws.s3.get('2', { type: 'json' });

          assert(response).eql({ a: 'a4' });
        });

        to('buffer', async () => {
          const response = await onesyAws.s3.get('3');

          assert(response.toString('utf-8')).eq('a');
        });

        to('Not found', async () => {
          const response = await onesyAws.s3.get('4');

          assert(response).eq(undefined);
        });

      });

      group('remove', async () => {

        group('options', () => {

          to('pure', async () => {
            const response = await onesyAws.s3.remove('1', { pure: true }) as any;
            const response1 = await onesyAws.s3.remove('2', { pure: false }) as any;

            assert(response).eql({});
            assert(await onesyAws.s3.get('1')).eq(undefined);

            addedItems.splice(0, 2);

            assert(response1).eq(undefined);
          });

        });

        to('remove', async () => {
          await onesyAws.s3.remove('3') as any;

          assert(await onesyAws.s3.get('3')).eq(undefined);

          addedItems.splice(0, 1);
        });

        to('Not found', async () => {
          await onesyAws.s3.remove('4') as any;
        });

      });

      group('removeMany', async () => {

        preTo(async () => {
          await onesyAws.s3.add('11', 'a');
          await onesyAws.s3.add('12', 'a');
          await onesyAws.s3.add('13', 'a');
          await onesyAws.s3.add('14', 'a');
        });

        group('options', () => {

          to('pure', async () => {
            const response = await onesyAws.s3.removeMany(['11', '12'], { pure: true }) as Array<any>;
            const response1 = await onesyAws.s3.removeMany(['13', '14'], { pure: false }) as Array<any>;

            assert(response).eql(new Array(2).fill({}));
            assert(await onesyAws.s3.get('11')).eq(undefined);
            assert(await onesyAws.s3.get('12')).eq(undefined);

            assert(response1).eql(new Array(2).fill(undefined));
          });

        });

        to('removeMany', async () => {
          await onesyAws.s3.removeMany(['11', '12']) as Array<any>;

          assert(await onesyAws.s3.get('11')).eq(undefined);
          assert(await onesyAws.s3.get('12')).eq(undefined);
        });

      });

    });

  });

});
