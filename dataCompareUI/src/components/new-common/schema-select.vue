<!-- src/components/business/schema-select.vue -->
<script setup lang="ts">
import { ref, toRefs, watch } from 'vue';
import { NSelect } from 'naive-ui';
import { getDBSchema } from '@/service/api';

const props = withDefaults(
  defineProps<{
    modelValue: string | null;
    datasourceId: number | null;
    filterable?: boolean;
    placeholder?: string;
  }>(),
  {
    filterable: true,
    placeholder: '请选择Schema'
  }
);

const emit = defineEmits(['update:modelValue']);

const { datasourceId } = toRefs(props);
const selectedSchema = ref(props.modelValue);
const loading = ref(false);
const options = ref<Array<{ value: string; label: string }>>([]);

watch(datasourceId, async newVal => {
  if (newVal === null || newVal === undefined) {
    options.value = [];
    selectedSchema.value = null;
    return;
  }

  try {
    loading.value = true;
    const { error, data } = await getDBSchema(newVal);
    if (!error) {
      options.value = data.map(schema => ({
        value: schema,
        label: schema
      }));
    }
  } finally {
    loading.value = false;
  }
});

function handleUpdate(value: string | null) {
  selectedSchema.value = value;
  emit('update:modelValue', value);
}
</script>

<template>
  <NSelect
    v-model:value="selectedSchema"
    :loading="loading"
    :filterable="filterable"
    :options="options"
    :placeholder="placeholder"
    clearable
    @update:value="handleUpdate"
  />
</template>
