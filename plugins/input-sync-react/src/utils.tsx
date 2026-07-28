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
  createScaffolderFieldExtension as createScaffolderFieldExtensionReact,
  FieldExtensionOptions,
  FieldExtensionComponent,
  CustomFieldValidator,
} from '@backstage/plugin-scaffolder-react';
import { Extension } from '@backstage/core-plugin-api';
import { UIOptionsType } from '@rjsf/utils';
import { FormFieldsSync } from './components';
import { syncUIOptions } from './components/FormFieldsSync/utils';
import { JsonValue } from '@backstage/types';
import nunjucks from 'nunjucks';
import { config, env } from './parser';

type ValidationHandler = {
  <TReturnValue, TInputProps>(
    validation: CustomFieldValidator<TReturnValue, TInputProps> | undefined,
  ): CustomFieldValidator<TReturnValue, TInputProps>;
};

export const isMultiExpressionTemplate = (template: string): boolean => {
  const { parser, nodes } = nunjucks as unknown as {
    parser: {
      parse(
        template: string,
        ctx: object,
        options: nunjucks.ConfigureOptions,
      ): { children: { children?: unknown[] }[] };
    };
    nodes: { TemplateData: Function };
  };

  const parsed = parser.parse(template, {}, config);

  return !(
    parsed.children.length === 1 &&
    !(parsed.children[0]?.children?.[0] instanceof nodes.TemplateData)
  );
};

export const evaluateTemplate = (
  template: string,
  context: object,
): JsonValue | undefined => {
  const trimmedTemplate = template.trim();

  try {
    if (isMultiExpressionTemplate(trimmedTemplate)) {
      return env.renderString(trimmedTemplate, context);
    }

    const templatePreservedType = template.replace(
      /\${{(.+)}}/g,
      '${{ ( $1 )  | dump }}',
    );

    const result = env.renderString(templatePreservedType, context);

    if (result === '') {
      return undefined;
    }

    const { value } = JSON.parse(`{"value": ${result}}`);

    return value;
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('parsing error', err);

    return template;
  }
};

export const validationUIOptionsSync: ValidationHandler =
  validation => async (value, errors, context) => {
    if (!validation) {
      return undefined;
    }

    const { formData, uiSchema } = context;

    if (!uiSchema || !uiSchema['ui:options']) {
      return validation(value, errors, context);
    }

    const uiOptions = uiSchema?.['ui:options'];
    const updatedUIOptions = JSON.parse(JSON.stringify(uiOptions));

    await syncUIOptions({
      formData,
      value: updatedUIOptions,
    });

    return validation(value, errors, {
      ...context,
      uiSchema: {
        ...context.uiSchema,
        'ui:options': updatedUIOptions,
      },
    });
  };

export function createScaffolderFieldExtension<
  TReturnValue = unknown,
  TInputProps extends UIOptionsType = {},
>({
  component: Component,
  validation,
  ...options
}: FieldExtensionOptions<TReturnValue, TInputProps>): Extension<
  FieldExtensionComponent<TReturnValue, TInputProps>
> {
  return createScaffolderFieldExtensionReact({
    ...options,
    component: (props: any) => (
      <FormFieldsSync component={Component} componentProps={props} />
    ),
    validation: validationUIOptionsSync(validation),
  });
}
