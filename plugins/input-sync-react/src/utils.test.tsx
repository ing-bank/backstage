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
import {
  createScaffolderFieldExtension,
  evaluateTemplate,
  validationUIOptionsSync,
} from './utils';

const TestComponent = () => <p>"hello world"</p>;

describe('utils', () => {
  describe('evaluateTemplate', () => {
    const testCases = [
      {
        input: {
          template: '${{ foo }}',
          context: {},
        },
        output: undefined,
      },
      {
        input: {
          template: 'hello ${{ foo }}',
          context: {},
        },
        output: 'hello ',
      },
      {
        input: {
          template: '${{ foo }}',
          context: {
            foo: null,
          },
        },
        output: null,
      },
      {
        input: {
          template: 'hello ${{ foo }} my friend',
          context: {
            foo: null,
          },
        },
        output: 'hello  my friend',
      },
      {
        input: {
          template: '${{ foo }}',
          context: {
            foo: 'hello',
          },
        },
        output: 'hello',
      },
      {
        input: {
          template: '${{ foo }} world',
          context: {
            foo: 'hello',
          },
        },
        output: 'hello world',
      },
      {
        input: {
          template: 'world ${{ foo }}',
          context: {
            foo: 'hello',
          },
        },
        output: 'world hello',
      },
      {
        input: {
          template: '${{ foo }}',
          context: {
            foo: 1,
          },
        },
        output: 1,
      },
      {
        input: {
          template: '${{ foo }}',
          context: {
            foo: true,
          },
        },
        output: true,
      },
      {
        input: {
          template: '${{ foo }}',
          context: {
            foo: false,
          },
        },
        output: false,
      },
      {
        input: {
          template: '${{ foo }}',
          context: {
            foo: {
              bar: 'test',
            },
          },
        },
        output: {
          bar: 'test',
        },
      },
      {
        input: {
          template: '${{ foo.bar }}',
          context: {
            foo: {
              bar: 'test',
            },
          },
        },
        output: 'test',
      },
      {
        input: {
          template: '${{ foo.barzz }}',
          context: {
            foo: {
              bar: 'test',
            },
          },
        },
        output: undefined,
      },
      {
        input: {
          template: '${{ foo }}',
          context: {
            foo: [
              {
                bar: 'test',
              },
            ],
            bar: null,
          },
        },
        output: [
          {
            bar: 'test',
          },
        ],
      },
      {
        input: {
          template: '${{ foo[0] }}',
          context: {
            foo: [
              {
                bar: 'test',
              },
            ],
            bar: null,
          },
        },
        output: {
          bar: 'test',
        },
      },
      {
        input: {
          template: '${{ foo[0].bar }}',
          context: {
            foo: [
              {
                bar: 'test',
              },
            ],
            bar: null,
          },
        },
        output: 'test',
      },
      {
        input: {
          template: '${{ foo[1].bar }}',
          context: {
            foo: [
              {
                bar: 'test',
              },
            ],
            bar: null,
          },
        },
        output: undefined,
      },
      {
        input: {
          template: '${{ foo }} ${{ bar }}',
          context: {
            foo: 'mugiwara',
            bar: 1,
          },
        },
        output: 'mugiwara 1',
      },
      {
        input: {
          template: '${{ foo }} no luffy ${{ bar }}',
          context: {
            foo: 'mugiwara',
            bar: true,
          },
        },
        output: 'mugiwara no luffy true',
      },
      {
        input: {
          template: '${{ foo | upper }} ',
          context: {
            foo: 'hello',
          },
        },
        output: 'HELLO',
      },
      {
        input: {
          template: "${{ foo }} ${{ ''}}",
          context: {
            foo: 'hello',
          },
        },
        output: 'hello ',
      },
      {
        input: {
          template: '${{ foo | capitalize }}',
          context: {
            foo: 'hello',
          },
        },
        output: 'Hello',
      },
      {
        input: {
          template: '${{ foo } ',
          context: {
            foo: 'hello',
          },
        },
        output: '${{ foo } ',
      },
      {
        input: {
          template: '${{ isEqual(1, 1) }}',
          context: {
            foo: 'hello',
          },
        },
        output: true,
      },
      {
        input: {
          template: '${{ isEqual(1, 1) }}',
          context: {
            isEqual: () => 'hello',
          },
        },
        output: 'hello',
      },
      {
        input: {
          template: "${{ bar if foo == 'hello' else 'no value' }}",
          context: {
            foo: 'hello',
            bar: 'world',
          },
        },
        output: 'world',
      },
    ];

    it.each(testCases)(
      'should return the correct value',
      ({ input: { template, context }, output }) => {
        const value = evaluateTemplate(template, context);

        expect(value).toEqual(output);
      },
    );
  });
  describe('validationUIOptionsSync', () => {
    const testCases = [
      {
        input: {
          uiSchema: undefined,
        },
        output: {
          uiSchema: undefined,
        },
      },
      {
        input: {
          uiSchema: null,
        },
        output: {
          uiSchema: null,
        },
      },
      {
        input: {
          uiSchema: {
            'ui:options': undefined,
          },
        },
        output: {
          uiSchema: {
            'ui:options': undefined,
          },
        },
      },
      {
        input: {
          formData: {
            organization: 'foo',
          },
          uiSchema: {
            'ui:options': null,
          },
        },
        output: {
          formData: {
            organization: 'foo',
          },
          uiSchema: {
            'ui:options': null,
          },
        },
      },
      {
        input: {
          formData: {
            organization: 'foo',
          },
          uiSchema: {
            'ui:options': {
              organization: 'bar',
            },
          },
        },
        output: {
          formData: {
            organization: 'foo',
          },
          uiSchema: {
            'ui:options': {
              organization: 'bar',
            },
          },
        },
      },
      {
        input: {
          formData: {
            organization: 'foo',
          },
          uiSchema: {
            'ui:options': {
              organization: '${{ organization }}',
            },
          },
        },
        output: {
          formData: {
            organization: 'foo',
          },
          uiSchema: {
            'ui:options': {
              organization: 'foo',
            },
          },
        },
      },
    ];

    describe.each(testCases)(
      'provided with the ff. test cases',
      ({ input: context, output: expectedContext }) => {
        describe(`given the context is ${JSON.stringify(context)} `, () => {
          it(`should have the updated context ${JSON.stringify(
            expectedContext,
          )} before validation`, async () => {
            const validation = jest.fn();
            const validationHandler = validationUIOptionsSync(validation);
            const value = '';
            const errors = { addError: () => {} };

            await validationHandler(value, errors, context as any);

            expect(validation).toHaveBeenCalledWith(
              value,
              errors,
              expectedContext as any,
            );
          });
        });
      },
    );

    it('should return when no validation is provided', async () => {
      const validationHandler = validationUIOptionsSync(undefined);
      const value = '';
      const errors = { addError: () => {} };

      await expect(
        validationHandler(value, errors, { uiSchema: {} } as any),
      ).resolves.toBeUndefined();
    });
  });

  describe('createScaffolderFieldExtension', () => {
    it('should return a valid component', () => {
      const Component = createScaffolderFieldExtension({
        name: 'TestComponent',
        component: TestComponent,
        schema: {
          returnValue: {},
          uiOptions: {},
        },
      });

      expect(Component).toBeDefined();
    });
  });
});
