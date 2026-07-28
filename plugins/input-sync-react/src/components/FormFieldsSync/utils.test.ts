/*
 * Copyright 2026 The Backstage Authors
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
import { JsonObject } from '@backstage/types';
import { getInterpolationFieldNames, syncUIOptions } from './utils';

describe('utils', () => {
  describe('getInterpolationFieldNames', () => {
    const testCases = [
      {
        inputs: [
          {
            uiOptions: {},
            formData: {
              bar: 'test',
            },
          },
          {
            uiOptions: null,
            formData: {
              bar: 'test',
            },
          },
          {
            uiOptions: undefined,
            formData: {
              bar: 'test',
            },
          },
        ],
        output: [],
      },
      {
        inputs: [
          {
            uiOptions: {
              foo: '${{bar}}',
              hello: 'world',
              isProd: false,
            },
            formData: {
              bar: 'test',
            },
          },
          {
            uiOptions: {
              foo: ['${{bar}}'],
            },
            formData: {
              bar: 'test',
            },
          },
          {
            uiOptions: {
              hello: {
                foo: '${{bar}}',
                hello: 'world',
              },
            },
            formData: {
              bar: 'test',
            },
          },
          {
            uiOptions: {
              hello: {
                foo: ['${{bar}}'],
              },
            },
            formData: {
              bar: 'test',
            },
          },
          {
            uiOptions: {
              hello: {
                foo: [false, { doe: '${{bar}}' }, 'hello world'],
              },
            },
            formData: {
              bar: 'test',
            },
          },
        ],
        output: ['bar'],
      },
      {
        inputs: [
          {
            uiOptions: {
              foo: '${{bar}}',
              hello: 'world',
              isProd: false,
              lorem: {
                isProd: '${{isProd}}',
                message: ['${{ hello.world}}'],
                labels: {
                  environment: 'this is env ${{ deployment.env[0] }}',
                },
              },
            },
            formData: {
              bar: 'test',
              isProd: false,
              hello: {
                world: 'test',
              },
              deployment: null,
            },
          },
        ],
        output: ['bar', 'isProd', 'hello', 'deployment'],
      },
    ];

    describe.each(testCases)(
      'should return form field names specified in the ui:options field interpolations',
      ({ inputs, output }) => {
        it.each(inputs)(
          'given the ui:options: %j',
          ({ uiOptions, formData }) => {
            const fields = getInterpolationFieldNames(formData, uiOptions);

            expect(Array.from(fields)).toEqual(output);
          },
        );
      },
    );
  });

  describe('syncUIOptions', () => {
    const testCases = [
      {
        input: {
          formData: {},
          uiOptions: {},
        },
        output: {},
      },
      {
        input: {
          formData: null,
          uiOptions: {},
        },
        output: {},
      },
      {
        input: {
          formData: {},
          uiOptions: null,
        },
        output: null,
      },
      {
        input: {
          formData: null,
          uiOptions: null,
        },
        output: null,
      },
      {
        input: {
          formData: undefined,
          uiOptions: undefined,
        },
        output: undefined,
      },
      {
        input: {
          formData: {},
          uiOptions: {
            foo: 'bar',
          },
        },
        output: {
          foo: 'bar',
        },
      },
      {
        input: {
          formData: {
            hello: 'world',
          },
          uiOptions: {
            foo: '${{ foo }}',
          },
        },
        output: {
          foo: undefined,
        },
      },
      {
        input: {
          formData: {
            hello: 'world',
            foo: null,
          },
          uiOptions: {
            foo: '${{ foo }}',
          },
        },
        output: {
          foo: null,
        },
      },
      {
        input: {
          formData: {
            firstName: 'foo',
            lastName: 'bar',
            age: 20,
            isMarried: true,
            spouse: {
              firstName: 'hello',
              lastName: 'hi',
              age: 10,
              hobbies: ['watching', 'dancing'],
            },
          },
          uiOptions: {
            name: {
              first: '${{firstName}}',
              fistAllCaps: '${{ firstName | upper }}',
              last: '${{lastName}}',
              full: '${{firstName}} ${{lastName}}',
              fullName: '${{firstName + lastName}}',
            },
            age: '${{ age }}',
            isMarried: '${{ isMarried }}',
            spouse: '${{ spouse }}',
            spouseFavoriteHobby: '${{ spouse.hobbies[0] }}',
            message:
              'Hello my name is ${{ firstName }} and my spouse is ${{ spouse.firstName }}',
            tags: [
              '${{ firstName }}',
              'isMarried=${{ isMarried}}',
              '${{ spouse.firstName }}',
              'spouseAge=${{spouse.age}}',
            ],
            cond: '${{ spouse.age if age == 20 else 0 }}',
          },
        },
        output: {
          name: {
            first: 'foo',
            fistAllCaps: 'FOO',
            last: 'bar',
            full: 'foo bar',
            fullName: 'foobar',
          },
          age: 20,
          isMarried: true,
          spouse: {
            firstName: 'hello',
            lastName: 'hi',
            age: 10,
            hobbies: ['watching', 'dancing'],
          },
          spouseFavoriteHobby: 'watching',
          message: 'Hello my name is foo and my spouse is hello',
          tags: ['foo', 'isMarried=true', 'hello', 'spouseAge=10'],
          cond: 10,
        },
      },
    ];

    it.each(testCases)(
      'should sync ui:options correctly',
      async ({ input: { formData, uiOptions }, output }) => {
        await syncUIOptions({
          formData: formData as JsonObject,
          parent: {},
          key: '',
          value: uiOptions,
        });

        expect(uiOptions).toEqual(output);
      },
    );
  });
});
