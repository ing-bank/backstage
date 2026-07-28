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
import { FieldExtensionComponentProps } from '@backstage/plugin-scaffolder-react';
import { FormFieldsSync } from './FormFieldsSync';
import { render, screen } from '@testing-library/react';

const TestChildComponent: React.FC<any> = (
  props: FieldExtensionComponentProps<string>,
) => <p>{JSON.stringify(props)}</p>;

type ComponentPropsOptions = {
  formData?: any;
  uiOptions?: any;
};

describe('FormFieldsSync', () => {
  const getComponentProps = ({
    formData,
    uiOptions,
  }: ComponentPropsOptions = {}) => ({
    uiSchema: {
      'ui:options': uiOptions,
    },
    formContext: {
      formData,
    },
    name: '',
    schema: {},
    idSchema: { $id: '' },
    onChange: () => {},
    onBlur: () => {},
    onFocus: () => {},
    disabled: false,
    readonly: false,
    registry: {} as any,
    rawErrors: [],
  });

  const getExpectedComponentProps = ({
    formData,
    uiOptions,
  }: ComponentPropsOptions = {}) =>
    JSON.stringify(
      getComponentProps({
        formData,
        uiOptions,
      }),
    );

  describe('should render correctly', () => {
    const testCases = [
      {
        input: {
          formData: {},
          uiOptions: {},
        },
        output: {
          uiOptions: {},
        },
      },
      {
        input: {
          formData: undefined,
          uiOptions: undefined,
        },
        output: {
          uiOptions: undefined,
        },
      },
      {
        input: {
          formData: null,
          uiOptions: null,
        },
        output: {
          uiOptions: null,
        },
      },
      {
        input: {
          formData: {
            organization: 'hello',
            project: 'world',
          },
          uiOptions: {
            organization: 'foo',
            project: 'bar',
          },
        },
        output: {
          uiOptions: {
            organization: 'foo',
            project: 'bar',
          },
        },
      },
      {
        input: {
          formData: {
            organization: 'foo',
            project: 'bar',
            empty: '',
            num: 1,
            bool: true,
            arr: [1, 2, 3],
            obj: {
              propA: 'a',
              propB: 'b',
              propC: [456, 'cb'],
            },
            nul: null,
          },
          uiOptions: {
            organization: '${{ organization }}',
            project: 'bar',
            empty: '${{ empty }}',
            num: '${{ num }}',
            bool: '${{ bool }}',
            arr: '${{ arr }}',
            arr1: '${{ arr[1] }}',
            obj: '${{ obj }}',
            objPropA: '${{ obj.propA }}',
            objPropC0: '${{ obj.propC[0] }}',
            nonExistent: '${{ nonExistent }}',
            invalidSyntax: '${ organization }',
            nul: '${{ nul }}',
          },
        },
        output: {
          uiOptions: {
            organization: 'foo',
            project: 'bar',
            empty: '',
            num: 1,
            bool: true,
            arr: [1, 2, 3],
            arr1: 2,
            obj: {
              propA: 'a',
              propB: 'b',
              propC: [456, 'cb'],
            },
            objPropA: 'a',
            objPropC0: 456,
            nonExistent: undefined,
            invalidSyntax: '${ organization }',
            nul: null,
          },
        },
      },
    ];

    describe.each(testCases)(
      'provided with the ff. test cases',
      ({ input, output }) => {
        describe(`given the component props: ${JSON.stringify(input)}`, () => {
          it(`then the child component should have updated ui:options: ${JSON.stringify(
            output.uiOptions,
          )}`, async () => {
            render(
              <FormFieldsSync
                component={TestChildComponent}
                componentProps={getComponentProps({
                  formData: input.formData,
                  uiOptions: input.uiOptions,
                })}
              />,
            );

            expect(
              await screen.findByText(
                getExpectedComponentProps({
                  formData: input.formData,
                  uiOptions: output.uiOptions,
                }),
              ),
            ).toBeDefined();
          });
        });
      },
    );
  });

  it('should render child with updated ui:options when form data changes', async () => {
    const { rerender } = render(
      <FormFieldsSync
        component={TestChildComponent}
        componentProps={getComponentProps({
          formData: {
            organization: 'hello',
            project: 'world',
          },
          uiOptions: {
            organization: '${{ organization }}',
            project: '${{ project }}',
          },
        })}
      />,
    );

    expect(
      await screen.findByText(
        getExpectedComponentProps({
          formData: {
            organization: 'hello',
            project: 'world',
          },
          uiOptions: {
            organization: 'hello',
            project: 'world',
          },
        }),
      ),
    ).toBeDefined();

    rerender(
      <FormFieldsSync
        component={TestChildComponent}
        componentProps={getComponentProps({
          formData: {
            organization: 'hello',
            project: 'test',
          },
          uiOptions: {
            organization: '${{ organization }}',
            project: '${{ project }}',
          },
        })}
      />,
    );

    expect(
      await screen.findByText(
        getExpectedComponentProps({
          formData: {
            organization: 'hello',
            project: 'test',
          },
          uiOptions: {
            organization: 'hello',
            project: 'test',
          },
        }),
      ),
    ).toBeDefined();
  });
});
