<template>
  <v-menu v-model="isOpen">
    <v-card dense >
      <v-card-title>Confirm Delete</v-card-title>
      <v-card-text>
        <v-btn small color='error' @click="$emit('confirm', item)">confirm</v-btn>
        <v-btn small class="ml-2" text @click="isOpen = false">cancel</v-btn>
      </v-card-text>
    </v-card>
    <template v-slot:activator="{ on }">
      {{ activator(on) }}
    </template>
  </v-menu>
</template>

<script>
  export default {
    name: 'delete-menu',
    props: ['item', 'activator'],
    data() {
      return {
        isOpen: false,
      }
    },
  }
</script>

<style scoped>

</style>
