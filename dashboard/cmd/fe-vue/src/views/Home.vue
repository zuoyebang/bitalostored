<template>
  <v-container fluid>
    <GlobalSearch
      :isOpen="isSearchOpen"
      @update:isOpen="isSearchOpen = $event"
      @focus="focusSearch" 
      @blur="blurSearch"
    />
    <v-tabs class="tabs" v-model="tab" align-with-title>
      <v-tabs-slider color="yellow"></v-tabs-slider>
      <v-tab v-for="item in items" :key="item">{{ item }}</v-tab>
    </v-tabs>

    <v-slider
      min="1"
      max="60"
      v-stream:input="inputIntervalTime$"
      :value="intervalTime"
      :label="'Interval: ' + intervalTime"
    />

    <template v-if="tab === 0">
    <v-card class="mb-2">
      <v-card-title>
        Overview
        <department></department>
      </v-card-title>
      <v-simple-table v-if="homeData">
        <tbody>
          <tr>
            <td>Product Name</td>
            <td>{{ homeData.config.product_name }}</td>
          </tr>
          <tr>
            <td>[Resource Pool]</td>
            <td><div v-html="homeData.cgroup"></div></td>
          </tr>
          <tr>
            <td>Dashboard</td>
            <td>
              Primary: <a :href="'//' + host + baseUrl + 'topomdirect?ha='+(homeData.master || homeData.model.hostport)" target="_blank">{{
                homeData.master || homeData.model.hostport
              }}</a>
              Standby: <template v-if="homeData.backup && homeData.backup.length">
                <template v-for="(backupAddr, index) in homeData.backup">
                  <a
                    :key="index"
                    :href="'//' +  host + baseUrl + 'topomdirect?ha='+ backupAddr"
                    target="_blank"
                    >{{ backupAddr }}</a
                  >
                  <span v-if="index < homeData.backup.length - 1"> | </span>
                </template>
              </template>
              <template v-else-if="homeData.model && homeData.model.backup_addr">
                <a
                  :href="'//' +  host + baseUrl + 'topomdirect?ha='+ homeData.model.backup_addr"
                  target="_blank"
                  >{{ homeData.model.backup_addr }}</a
                >
              </template>
              <span v-else>-</span>
            </td>
          </tr>
          <tr>
            <td>Config</td>
            <td>
              <a
                :href="'//'+homeData.config.paas_host+ baseUrl +'bitalospaas/static/#/config?clusterName=' + homeData.config.product_name+ '&serviceId=6&type=update config'"
                target="_blank"
                >bitalos-config
                </a>
                >|<
              <a
                :href="'//'+homeData.config.paas_host+ baseUrl +'bitalospaas/static/#/config?clusterName=' + homeData.config.product_name+ '&serviceId=2&type=update config'"
                target="_blank"
                >proxy-config
                </a>
            </td>
          </tr>
          <tr>
            <td>Proxy Monitor</td>
            <td v-if="homeData.monitorLink">
              <a v-if="homeData.monitorLink.tx"
                :href="homeData.monitorLink.tx"
                target="_blank"
              >TX
              </a>
              <span v-else>-</span>
              |
              <a v-if="homeData.monitorLink.ali"
                :href="homeData.monitorLink.ali"
                target="_blank"
              >ALI
              </a>
              <span v-else>-</span>
              |
              <a v-if="homeData.monitorLink.bd"
                :href="homeData.monitorLink.bd"
                target="_blank"
              >BD
              </a>
              <span v-else></span>
            </td>
          </tr>
        </tbody>
      </v-simple-table>
    </v-card>
    <LineChart :chartData="overviewChartData" />
    <v-card class="mt-2">
      <v-card-title class="slots">
        Slots
        <div class="right">
          <app-modal
            ref="modal"
            title=""
            :width="500"
            :content="searchRes"
            :activator="() => {}"
          >
          </app-modal>
          <v-text-field
            class="input"
            label="key"
            v-model="slots"
          ></v-text-field>
          <v-btn color="primary" class="btn" @click="searchSlots"
            >search slot</v-btn
          >
          <app-menu
            title="init slots"
            :activator="() => {}"
            @confirm="initSlot"
            ref="menu"
          >
          </app-menu>
          <v-btn color="success" class="btn" @click="showInit"
            >init slots</v-btn
          >
        </div>
      </v-card-title>
      <SlotsControl v-stream:update="update$" />
      <SlotsView v-if="!!state && !!state.slots" :list="state.slots" />
    </v-card>
    <v-card class="mt-2">
      <MigrateTable
        :slotAction="state && state.slot_action"
        :list="migrateTableList"
      />
    </v-card>
    <v-card class="mt-2">
      <V8SlotHistoryRender
      :list="v8SlotHistoryData"
      />
    </v-card>
    </template>

    <template v-if="tab === 1">
    <Proxy
      v-if="!!state && !!state.proxy"
      :modelsList="state.proxy.models"
      :statsList="state.proxy.stats"
      :readCrossCloud="state.read_cross_cloud"
      v-stream:update="update$"
    />
    </template>
    
    <template v-if="tab === 2">
    <v-card class="mt-2">
      <v-card-title>Group</v-card-title>
      <GroupControl
        v-if="state && state.group && homeData"
        v-stream:update="update$"
        :models="state.group.models"
      />
      <GroupView
        v-stream:update="update$"
        :host="homeData.model.hostport"
        :models="state.group.models"
        :stats="state.group.stats"
        :server_stats="state.group.server_stats"
        v-if="state && state.group && homeData"
      />
     
    </v-card>
    <Dk/>
    </template>
  </v-container>
</template>

<script lang="ts">
import {
  pluck,
  mergeMapTo,
  tap,
  debounceTime,
  map,
  filter,
} from "rxjs/operators";
import {
  getHomeData$,
  getHomeStats$,
  getMigrateTable$,
  initSlots,
  searchSlot,
  V8SlotHistoryApi,
} from "@/api";
import V8SlotHistoryRender from "@/views/home/v8-slots";
import { interval } from "rxjs";
import LineChart from "./home/line-chart";
import Proxy from "./home/proxy";
import SlotsView from "./home/slots-view";
import SlotsControl from "./home/slots-control";
import V8SlotHistory from "./home/v8-slots";
import moment from "moment";
import AppMenu from "@/components/app-menu";
import AppModal from "@/components/app-modal";
import GroupView from "@/views/home/group-view";
import GroupControl from "@/views/home/group-control";
import Dk from "@/views/home/dk";
import MigrateTable from "@/views/home/migrate-table";
import Department from "@/views/home/department";
import GlobalSearch from "@/components/Search.vue";


export default {
  components: {
    V8SlotHistoryRender,
    MigrateTable,
    GlobalSearch,
    GroupControl,
    GroupView,
    Proxy,
    LineChart,
    SlotsView,
    SlotsControl,
    Dk,
    AppModal,
    AppMenu,
    Department,
  },
  name: "Home",
  data() {
    return {
      isSearchFocused: false,
      isSearchOpen: false,
      tab: 0,
      items: [
        'Overview', 'Proxy', 'Group',
      ],
      intervalTime: 0,
      state: {},
      overviewChartData: [],
      migrateTableList: [],
      timer: "",
      slots: "",
      searchRes: "",
      host: window.location.host,
      baseUrl: process.env.BASE_URL || '/',
      v8SlotHistoryData: [],
    };
  },
  mounted() {
    document.addEventListener('keydown', this.handleKeyPress);
  },
  beforeDestroy() {
    document.removeEventListener('keydown', this.handleKeyPress);
  },
  methods: {
    focusSearch() {
      this.isSearchFocused = true; // search overlay focused
    },
    blurSearch() {
      this.isSearchFocused = false;
    },
    handleKeyPress(event) {
      const isInputFocused = document.activeElement.tagName === 'INPUT' || document.activeElement.tagName === 'TEXTAREA';
      if (!this.isSearchFocused && !isInputFocused) {
        if (event.key.toLowerCase() === 's') {
          event.preventDefault();
          this.isSearchOpen = true;
        }
      }
      if (event.key === 'Escape') {
        this.isSearchOpen = false;
      }
    },
    async initSlot() {
      const data = await initSlots();
    },
    async searchSlots() {
      if (!this.slots) return;
      const data = await searchSlot(this.slots);
      this.searchRes = data.data.data;
      this.$refs.modal.isOpen = true;
    },
    showInit() {
      this.$refs.menu.isOpen = true;
    },
  },
  created(): void {
    this.intervalTime = 10;
  },
  domStreams: ["inputIntervalTime$", "update$"],
  subscriptions() {
    // refresh data
    this.update$
      .pipe(mergeMapTo(getHomeStats$()), pluck("data"))
      .subscribe((d) => (this.state = d));
    // poll by interval
    let interval$;
    this.$watchAsObservable("intervalTime")
      .pipe(
        debounceTime(500),
        pluck("newValue"),
        tap(
          () => interval$ && interval$.unsubscribe && interval$.unsubscribe()
        ),
        tap((t) => console.log(`Refresh interval: ${t}s`))
      )
      .subscribe((t: number) => {
        interval$ = interval(t * 1000)
          .pipe(
            mergeMapTo(getHomeStats$()),
            pluck("data"),
            map((d) => (this.state = d)),
            mergeMapTo(getMigrateTable$()),
            pluck("data"),
            map((d) => (this.migrateTableList = d)),
            mergeMapTo(V8SlotHistoryApi()),
            pluck("data"),
            map((d) => (this.v8SlotHistoryData = d)),
          )
          .subscribe();
      });
    // line chart
    this.$watchAsObservable("state")
      .pipe(
        pluck("newValue", "proxy", "stats"),
        filter((d) => !!d),
        map((d) => Object.values(d)),
        map((item: any[]) =>
          item.reduce(
            (
              total,
              {
                stats: {
                  cdm_ops: cdmOps,
                },
              }
            ) => {
              total.cmd += cdmOps.qps;
              return total;
            },
            { cmd: 0}
          )
        )
      )
      .subscribe(
        (item) =>
          (this.overviewChartData = {
            ...item,
            label: moment().format("HH:mm:ss"),
          })
      );
    return {
      v8SlotHistoryData: V8SlotHistoryApi().pipe(pluck("data")),
      migrateTableList: getMigrateTable$().pipe(pluck("data")),
      state: getHomeStats$().pipe(
        pluck("data")
        // tap(console.log),
      ),
      intervalTime: this.inputIntervalTime$.pipe(pluck("event", "msg")),
      homeData: getHomeData$().pipe(
        pluck("data")
        // tap(console.log),
      ),
      
    };
  },
  computed: {},
  destroyed() {
    for (let i = 0; i < 9999; i++) {
      clearInterval(i);
    }
  },
};
</script>
<style lang="scss">
.tabs {
  margin-bottom: 20px;
}
.slots {
  display: flex;
  justify-content: space-between;
}
.input {
  display: inline-block;
}
.btn {
  margin-left: 10px;
}
.v-list-group__header__append-icon {
  min-width: 24px !important;
}
.v-dialog {
  background-color: white;
}
.data-text {
  min-height: 200px;
  border: 1px solid;
  white-space: pre-wrap;
  overflow: scroll;
}
</style>
