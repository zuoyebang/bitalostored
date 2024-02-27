<template>
  <v-card>
    <v-card-title class="subheading font-weight-bold">
      {{ title }}: {{count}}
    </v-card-title>
    <v-divider></v-divider>
    <v-list>
      <v-list-item v-for="i in list" :key="i">
        <v-list-item-content>{{i}}</v-list-item-content>
      </v-list-item>
    </v-list>
  </v-card>
</template>

<script>
export default {
  name: 'table.vue',
  props: {
    title: String,
    list: {
      required: true,
      default: []
    },
    count: Number,
  }
}
</script>

<style scoped lang="scss">
  .table {
    display: inline-block;
    padding: 5px 10px;
    border: 1px solid #ddd;
    margin: 10px;
    width: 150px;
    word-break:break-all;
    .title {
      text-align: center;
    }
  }
</style>
