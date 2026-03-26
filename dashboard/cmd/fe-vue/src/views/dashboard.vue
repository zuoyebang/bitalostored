<template>
  <v-container fluid>
  <GlobalSearch
    :isOpen="isSearchOpen"
    @update:isOpen="isSearchOpen = $event"
    @focus="focusSearch" 
    @blur="blurSearch"
  />
  <div class="home-page">
    <!--  <div>
      <v-row>
        <v-col
          :key="i.name"
          v-for="i in info.total"
        >
          <Ftable
            :title="i.name"
            :list="i.detail"
            :count="i.count"
          ></Ftable>
        </v-col>
      </v-row>
    </div> -->
    <p class="home-page__title">Overview</p>
    <div class="overview">
      <span>Total clusters: {{ overview.totalCluster }}</span>
      <span>total Proxy Qps：{{ overview.totalProxyQps }}</span>
      <span>total Server Qps：{{ overview.totalServerQps }}</span>
    </div>
    <p class="home-page__title">
      Pending exceptions
      <a class="exception-link" @click.stop="handleClickLinkExceptionHistory">Exception history</a>
      <span class="link-separator"> </span>
      <a class="exception-link" @click.stop="handleClickLinkRiskKey">Risk keys</a>
      <span class="link-separator"> </span>
      <a class="exception-link" @click.stop="handleClickLinkWiki">Metrics wiki</a>
      <span class="link-separator"> </span>
      <v-btn
        color="primary"
        small
        @click.stop="handleClickIgnoreAll"
        class="ma-1"
      >
        Ignore all
      </v-btn>
    </p>
    <v-simple-table height="450px" class="exception-table">
      <thead>
        <tr>
          <th class="text-left">Cluster</th>
          <th class="text-left">Status</th>
          <th class="text-left operate">Actions</th>
          <th class="text-left">Alerts</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in exceptionList" :key="item.name">
          <td>
            <a :href="item.link" target="_blank">{{ item.name }}</a>
          </td>
          <td>
            <div v-if="item.status === 0">Pending</div>
            <div v-else>In progress</div>
          </td>
          <td>
            <div v-if="item.status === 0">
              <span
                class="operate-btn"
                @click.stop="handleClickOperate(item, operateItem.status)"
                v-for="operateItem in operateList"
                :key="item.name + operateItem.status"
                >{{ operateItem.name }}</span
              >
            </div>
            <div v-else-if="item.status === 1">
              <span
                class="operate-btn"
                @click.stop="handleClickOperate(item, operateItem.status)"
                v-for="operateItem in operateList"
                :key="item.name + operateItem.status"
                >{{ operateItem.name }}</span
              >
            </div>
          </td>
          <td>
            <div v-for="elem in item.exceptionInfos" :key="elem.name">
              {{ elem }}
            </div>
          </td>
        </tr>
      </tbody>
    </v-simple-table>
    <p class="home-page__title">Clusters by department</p>
    <v-expansion-panels v-model="panel" multiple>
      <v-expansion-panel
        v-for="(clusterVal, clusterKey, index) in departmentCluster"
        :key="clusterKey"
      >
        <v-expansion-panel-header>
          {{ clusterKey }}
        </v-expansion-panel-header>
        <v-expansion-panel-content>
          <template v-if="panel.includes(index)">
            <div
              v-for="content in clusterVal"
              :key="content.clusterName"
              class="cluster"
            >
              <p class="cluster-name">
                Cluster: <a :href="content.clusterLink" target="_blank">{{
                  content.clusterName
                }}</a>
              </p>
              <template
                v-for="(item, key, index) in {
                  proxy: content.proxy,
                  server: content.server,
                }"
              >
                <span :key="index + 'info'" class="cluster-info">
                  {{ key }}:{{ item.info }}
                </span>
                <v-simple-table :key="index">
                  <thead>
                    <tr>
                      <th class="text-left">Name</th>
                      <th class="text-left">qps</th>
                      <th class="text-left">cpu</th>
                      <th class="text-left">mem</th>
                      <th class="text-left">disk</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="(proxyVal, proxyKey) in item" :key="proxyKey">
                      <template v-if="proxyKey !== 'info'">
                        <td>
                          {{ proxyKey }}
                        </td>
                        <td>{{ proxyVal.qps }}</td>
                        <td>{{ proxyVal.cpu }}</td>
                        <td>{{ proxyVal.mem }}</td>
                        <td>{{ proxyVal.disk }}</td>
                      </template>
                    </tr>
                  </tbody>
                </v-simple-table>
              </template>

              <div>
                Status: <v-btn
                  :color="content.status ? 'error' : 'success'"
                  class="ma-2"
                >
                  {{ content.status ? "Abnormal" : "Normal" }}
                </v-btn>
              </div>
              <span class="exception-info"> {{ content.exceptionInfo }}</span>
            </div>
          </template>
        </v-expansion-panel-content>
      </v-expansion-panel>
    </v-expansion-panels>
  </div>
</v-container>
</template>
<script>
import { getHomepageData, handleException } from "../api";
import GlobalSearch from "@/components/Search.vue"

export default {
  name: "dashboard",
  components: {
    GlobalSearch,
  },
  data() {
    return {
      isSearchFocused: false,
      isSearchOpen: false,
      panel: [],
      overview: { totalCluster: 0, totalProxyQps: 0, totalServerQps: 0 },
      exceptionList: [],
      departmentCluster: {},
      timer: null,
      operateList: [
        { name: "In progress", status: 1 },
        { name: "Resolved", status: 2 },
        { name: "Ignore", status: 3 },
      ],
      paasDomain: "",
    };
  },
  methods: {
    focusSearch() {
      this.isSearchFocused = true;
    },
    blurSearch() {
      this.isSearchFocused = false;
    },
    handleKeyPress(event) {
      if (!this.isSearchFocused) {
        if (event.key.toLowerCase() === 's') {
          event.preventDefault();
          this.isSearchOpen = true;
        }
      }
      if (event.key === 'Escape') {
        this.isSearchOpen = false;
      }
    },
    async getData() {
      const res = await getHomepageData();
      const { overview, exceptionList, departmentCluster } = res.data.data;
      this.overview = overview;
      this.exceptionList = exceptionList;
      this.departmentCluster = departmentCluster;
      
      if (overview && overview.paasDomain) {
        this.paasDomain = overview.paasDomain;
      } else {
        this.paasDomain = window.location.origin;
      }
      
      this.timer = setTimeout(() => {
        this.getData();
      }, 5e3);
    },
    handleClickOperate(item, status) {
      handleException({ name: item.name, status }).then((res) => {
        if (res && res.status === 200) {
          clearTimeout(this.timer);
          this.getData();
          this.$message({
            message: "Success",
            type: "success",
          });
        }
      });
    },
    handleClickLinkExceptionHistory() {
      window.open(
        `${this.paasDomain}/storedpaas/static/#/exception-history`,
        "_blank"
      );
    },
    handleClickLinkRiskKey() {
      window.open(
        `${this.paasDomain}/storedpaas/static/#/risk-key`,
        "_blank"
      );
    },
    handleClickLinkWiki() {
      window.open(
        "https://docs.zuoyebang.cc/doc?fileId=1829474545908990367",
        "_blank"
      );
    },
    handleClickIgnoreAll() {
      handleException({ name: "", status: 3 }).then((res) => {
        if (res && res.status === 200) {
          clearTimeout(this.timer);
          this.getData();
          this.$message({
            message: "Success",
            type: "success",
          });
        }
      });
    },
    transStatus(status) {
      let res = "";
      switch (status) {
        case 0:
          res = "Pending";
          break;
        case 1:
          res = "In progress";
          break;
        case 2:
          res = "Resolved";
          break;
        case 3:
          res = "Ignored";
      }
      return res;
    },
  },
  created() {
    this.getData();
  },
  mounted() {
    document.addEventListener('keydown', this.handleKeyPress);
  },
  beforeDestroy() {
    document.removeEventListener('keydown', this.handleKeyPress);
  },
  destroyed() {
    clearTimeout(this.timer);
  },
};
</script>

<style scoped >
.home-page {
  padding: 0 40px;
}
.home-page__title {
  margin-top: 40px;
  font-size: 28px;
  font-weight: bold;
}
.overview {
  display: flex;
  justify-content: space-between;
}
.exception-table {
  box-shadow: 0px 3px 1px -2px rgb(0 0 0 / 20%),
    0px 2px 2px 0px rgb(0 0 0 / 14%), 0px 1px 5px 0px rgb(0 0 0 / 12%);
  border-radius: 4px;
}
.cluster-name {
  padding-left: 16px;
}
.cluster-info {
  padding-left: 16px;
  margin-bottom: 8px;
  display: inline-block;
}
.cluster {
  vertical-align: top;
  width: 400px;
  display: inline-block;
  box-shadow: 0px 3px 1px -2px rgb(0 0 0 / 20%),
    0px 2px 2px 0px rgb(0 0 0 / 14%), 0px 1px 5px 0px rgb(0 0 0 / 12%);
  border-radius: 4px;
  padding: 10px;
  margin: 0 10px 10px 0;
}
.exception-info {
  word-break: break-word;
}
.operate {
  width: 200px;
}
.operate-btn {
  color: #1976d2;
  text-decoration: underline;
  margin-right: 10px;
  cursor: pointer;
}
.exception-link {
  font-size: 16px;
  margin-right: 10px;
}
.link-separator {
  margin-right: 20px;
}
</style>
