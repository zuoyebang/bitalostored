import Vue from 'vue'
import { getStoreList$ } from '@/api'
import { map } from 'rxjs/operators'
import LogDialog from '@/views/app/log-dialog'
import LogView from '@/views/app/log-view'
import Login from '@/views/Login'

const Main = Vue.extend({
  subscriptions() {
    return {
      navList: getStoreList$()
        .pipe(map(({ data: d }) => d.data)),
    }
  },
  render() {
    return <v-app>
      {this.$route.name !== 'Login' && <v-navigation-drawer
        permanent={true}
        app={true}
        width={200}
      >
        <v-toolbar color="primary darken-1" dark={true} short={true}>Bitalosdashboard</v-toolbar>
        <v-list>
          <v-list-item to={'index'}>
            Home
          </v-list-item>
          {
            this.navList && this.navList.map((i) =>
              <v-list-group>
                <template slot="activator">
                  <v-list-item-content>
                    <v-list-item-title>{i.departmentName}</v-list-item-title>
                  </v-list-item-content>
                </template>
                {
                  i.clusterList.map((i) =>
                    <v-list-item to={i}>
                      <v-list-item-content>
                        <v-list-item-title>{i}</v-list-item-title>
                      </v-list-item-content>
                    </v-list-item>
                  )
                }
              </v-list-group>)
          }
        </v-list>
        <LogView />
      </v-navigation-drawer>}
      {this.$route.name !== 'Login' && <v-app-bar
        app
        color="primary"
        short
      >
        <v-spacer></v-spacer>
      </v-app-bar>}
      <v-main>
        <router-view key={this.$route.path + new Date().getTime()} />
      </v-main>
      <LogDialog />
    </v-app>
  },
})

export default Vue.extend({
  render() {
    return this.$route.name !== 'Login' ? <Main /> : <Login />
  },
})

