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
import { ComponentType, useEffect, useMemo, useState } from 'react';
import useDebounce from 'react-use/lib/useDebounce';
import { getInterpolationFieldNames, syncUIOptions } from './utils';

import { FieldExtensionComponentProps } from '@backstage/plugin-scaffolder-react';
import { JsonObject } from '@backstage/types';

type FormFieldsSyncProps<P extends FieldExtensionComponentProps<string>> = {
  component: ComponentType<P>;
  componentProps: P;
};

export function FormFieldsSync<P extends FieldExtensionComponentProps<string>>({
  component: Component,
  componentProps,
}: FormFieldsSyncProps<P>) {
  const { uiSchema, formContext } = componentProps;
  const uiOptions = uiSchema['ui:options'] as JsonObject;
  const fieldValues = useMemo(
    () =>
      JSON.stringify(
        Array.from(
          getInterpolationFieldNames(formContext.formData, uiOptions),
        ).map(field => formContext.formData[field]),
      ),
    [formContext.formData, uiOptions],
  );

  const [syncedUIOptions, setSyncedUIOptions] = useState(uiOptions);
  const [optionsSynced, setOptionsSynced] = useState<boolean>(false);

  useEffect(() => {
    const syncComponentUIOptions = async () => {
      if (!uiOptions) {
        setOptionsSynced(true);
        return;
      }

      const updatedUIOptions = JSON.parse(JSON.stringify(uiOptions));

      await syncUIOptions({
        formData: formContext.formData,
        value: updatedUIOptions,
      });

      setSyncedUIOptions(updatedUIOptions);
      setOptionsSynced(true);
    };

    syncComponentUIOptions();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useDebounce(
    () => {
      const syncComponentUIOptions = async () => {
        if (!uiOptions) {
          return;
        }

        const updatedUIOptions = JSON.parse(JSON.stringify(uiOptions));

        await syncUIOptions({
          formData: formContext.formData,
          value: updatedUIOptions,
        });

        setSyncedUIOptions(updatedUIOptions);
      };

      syncComponentUIOptions();
    },
    500,
    [fieldValues],
  );

  const updatedComponentProps = {
    ...componentProps,
    uiSchema: {
      ...componentProps.uiSchema,
      'ui:options': syncedUIOptions,
    },
  };

  return Component && optionsSynced && <Component {...updatedComponentProps} />;
}
