<template>
  <transition name="fade">
    <div v-if="isOpen" class="global-search-overlay" @click="handleOutsideClick">
      <div class="global-search" @click.stop>
        <input
          type="text"
          v-model="searchQuery"
          placeholder="Enter cluster name"
          @keyup.enter="performSearch"
          ref="searchInput"
          @focus="$emit('focus')"
          @blur="$emit('blur')"
          autofocus
        />
        <div class="hint">Press <kbd class="custom-kbd">s</kbd> to open, <kbd class="custom-kbd">ESC</kbd> to close</div>
        <div class="search-results" v-if="filterItems.length">
          <ul>
            <li
              v-for="item in filterItems"
              :key="item.departmentName"
              @click="goToCluster(item.route)"
            >
            <div class="department-name">{{ item.departmentName }}</div>
      <div class="cluster-list">
        <span
          v-for="(cluster, index) in item.clusterList"
          :key="index"
          @click="goToCluster(cluster)"
          class="cluster-item"
        >
          {{ cluster }}
        </span>
      </div>
            </li>
          </ul>
        </div>
      </div>
    </div>
  </transition>
</template>

<script>
import {getStoreList$} from '@/api'
import {map} from 'rxjs/operators'
export default {
  props: {
    isOpen: Boolean,
  },
  data() {
    return {
      searchQuery: '',
      menuItems: [],
    };
  },
  computed: {
    filterItems() {
      if (!this.searchQuery) {
        return this.menuItems;
      }
      const lowerQuery = this.searchQuery.toLowerCase();
      const filteredMenuItems = this.menuItems.map(item => ({...item,
        clusterList: item.clusterList.filter(cluster => cluster.toLowerCase().includes(lowerQuery))
      })).filter(item => 
      item.clusterList.length > 0);
      return filteredMenuItems
    },
  },
  methods: {
    performSearch() {
      this.$emit('search', this.searchQuery);
    },
    closeSearch() {
      this.$emit('update:isOpen', false);
    },
    goToCluster(route) {
      if (route !== undefined) {
      this.$router.push(route);
      this.closeSearch();
    } else {
    console.error('Route is undefined');
    }
    },
    fetchMenuItems() {
      this.menuItems = getStoreList$().pipe(map(({data: d}) => d.data));
    },
    handleOutsideClick(event) {
      if (!this.$el.contains(event.target)) {
        this.closeSearch();
      }
    },
  },
  watch: {
    isOpen(newVal) {
      if (newVal) {
        this.searchQuery = ''; // clear input when opening search
        this.$nextTick(() => {
          this.$refs.searchInput.focus(); // focus input
        });
      }
    },
  },
  created() {
    //this.fetchMenuItems(); // fetch menu on create
  },
  mounted() {
    document.addEventListener('click', this.handleOutsideClick);
    getStoreList$().pipe(
      map(({ data: d }) => d.data), // extract payload
    ).subscribe({
      next: (menuData) => {
        this.menuItems = menuData; // assign to reactive list
      },
      error: (err) => {
        console.error("Error fetching or filtering data:", err);
      }
    });
  },
  beforeDestroy() {
    document.removeEventListener('click', this.handleOutsideClick);
  },
};
</script>

<style scoped>
.global-search-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(0, 0, 0, 0.5); /* dim backdrop */
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
}

.global-search {
  background: white;
  padding: 20px;
  border: 1px solid #ccc;
  border-radius: 5px;
  width: 60%; /* search panel width */
  max-width: 800px; /* max width */
}

.global-search input {
  width: 100%; /* input width */
  padding: 10px;
  font-size: 16px;
}

.global-search button {
  margin-left: 10px;
  padding: 10px;
  font-size: 16px;
}

/* fade transition */
.fade-enter-active, .fade-leave-active {
  transition: opacity 0.5s;
}
.fade-enter, .fade-leave-to {
  opacity: 0;
}

.hint {
  text-align: center;
  margin-top: 10px;
  margin-bottom: 10px;
  color: #888;
  font-size: 14px;
}

.custom-kbd {
  font-size: inherit;
  color: inherit;
  padding: 0;
  margin: 0;
  background: none;
  border: none;
  box-shadow: none;
  display: inline;
}

.search-results {
  max-height: 400px;
  overflow-y: auto;
  overflow-x: hidden;
  border: 1px solid #ccc;
  border-radius: 4px;
  padding: 10px;
}

.search-results ul {
  list-style-type: none;
  padding: 0;
  margin: 0;
}

.search-results li {
  display: block;
  align-items: center;
  padding: 8px;
  border-bottom: 1px solid #eee;
}

.search-results li:last-child {
  border-bottom: none;
}

.search-results .department-name {
  font-weight: bold;
  margin-bottom: 8px;
  display: flex;
}

.search-results .cluster-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.search-results .cluster-item {
  background-color: #f0f0f0;
  padding: 4px 8px;
  border-radius: 4px;
  cursor: pointer;
  transition: background-color 0.3s;
  display: inline-block;
}

.search-results .cluster-item:hover {
  background-color: #e0e0e0; /* hover */
}
</style>