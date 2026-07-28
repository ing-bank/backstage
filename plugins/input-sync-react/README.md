# Input Sync Plugin

This plugin provides the Backstage template input field the ability to access and use the values of other input fields as property of its ui:options

## Getting Started

### Installation

```sh
cd services/app/packages/app
yarn add @internal/backstage-plugin-input-sync-react@0.1.0
```

### Configuration

When creating a custom input field extension, instead of using the backstage scaffolder utility function, we use the scaffolder utility function provided by this plugin

```ts
// import { createScaffolderFieldExtension } from "@backstage/plugin-scaffolder-react";
import { createScaffolderFieldExtension } from '@internal/backstage-plugin-input-sync-react';
```

```ts
export const CustomPickerExtension = scaffolderPlugin.provide(
  createScaffolderFieldExtension({
    component: CustomPicker,
    name: 'CustomPicker',
    schema: CustomPickerSchema,
    validation: CustomPickerValidation,
  }),
);
```

Once configured, the custom input field will have the ability to use the values of other custom input fields in its ui:options.

### Features

Given the CustomPicker input field specified above, this is how we could sync the other custom input fields to its ui:options.

It supports the data types: string, number, boolean, object and array.

```yaml
customPicker:
  title: Custom Picker
  type: string
  ui:field: CustomPicker
  ui:options:
    text: ${{ text }}
    num: ${{ num }}
    bool: ${{ isBool }}
    obj: ${{ obj }}
    arr: ${{ arr }}
    foo:
      bar: ${{ fooBar }}
```

It also supports concatenating string and field values.

```yaml
customPicker:
  title: Custom Picker
  type: string
  ui:field: CustomPicker
  ui:options:
    message: ${{ text }} ${{ num }}
    messageA: Am I a robot? ${{ isRobot }}
```

It also supports access to object properties.

```yaml
customPicker:
  title: Custom Picker
  type: string
  ui:field: CustomPicker
  ui:options:
    obj: ${{ obj.propA }}
    text: ${{ obj.propA.subPropA }}
    foo:
      bar: ${{ obj.propA }}
```

It also supports access to array elements.

```yaml
customPicker:
  title: Custom Picker
  type: string
  ui:field: CustomPicker
  ui:options:
    itemA: ${{ arr[0] }}
    itemB: ${{ arr[0].text }}
    itemC: ${{ arr[0].arrA[2] }}
```
