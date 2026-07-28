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
import { JsonArray, JsonObject, JsonValue } from '@backstage/types';
import { isNil } from 'lodash';
import { evaluateTemplate } from '../../utils';

export const getInterpolationFieldNames = (
  formData: JsonObject,
  value?: JsonValue,
): Set<string> => {
  const names = new Set<string>();

  if (!!value && typeof value === 'object') {
    Object.values(value)
      .flatMap(v => Array.from(getInterpolationFieldNames(formData, v)))
      .forEach(name => names.add(name));
  }

  if (typeof value !== 'string' || value.trim() === '') {
    return names;
  }

  Object.keys(formData)
    .filter(fieldName => value.includes(fieldName))
    .forEach(name => names.add(name));

  return names;
};

export const syncUIOptions = async ({
  formData,
  parent,
  key,
  value,
}: {
  formData: JsonObject;
  parent?: JsonObject | JsonArray;
  key?: string;
  value?: JsonValue;
}) => {
  if (!!value && typeof value === 'object') {
    for (const [k, v] of Object.entries(value)) {
      await syncUIOptions({
        formData,
        parent: value,
        key: k,
        value: v,
      });
    }

    return;
  }

  if (isNil(value) || typeof value !== 'string' || value.trim() === '') {
    return;
  }

  const updatedValue = evaluateTemplate(value, formData);

  (parent as JsonObject)[key as string] = updatedValue;
};
