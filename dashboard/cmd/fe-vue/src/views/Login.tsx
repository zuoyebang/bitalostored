import {Component, Vue} from 'vue-property-decorator'
import {login$} from '@/api'
import {UserNamespace} from '@/store'
import {SET_ROLE} from '@/store/types'

@Component
export default class Login extends Vue {
  username = ''
  password = ''

  @UserNamespace.Action(SET_ROLE)
  setRole

  render() {
    return (
      <v-app>
        <v-container class='d-flex justify-center align-center'>
          <v-card width={500} class='mt-5'>
            <v-card-title>Login</v-card-title>
            <v-card-text>
              <v-row>
                <v-col>
                  <v-text-field
                    label='Username'
                    autofocus
                    oninput={(val: string) => this.username = val}
                    value={this.username}
                  />
                </v-col>
              </v-row>
              <v-row>
                <v-col>
                  <v-text-field label='Password' oninput={(val: string) => this.password = val} value={this.password}/>
                </v-col>
              </v-row>
              <v-row>
                <v-col>
                  <v-btn color='primary' onclick={this.login}>Login</v-btn>
                </v-col>
              </v-row>
            </v-card-text>
          </v-card>
        </v-container>
      </v-app>
    )
  }

  async login() {
    const {username, password} = this
    try {
      const {response: {errmsg, data}} = await login$({username, password}).toPromise()
      if (!errmsg.cause) {
        this.setRole(data.role)
        this.$router.push('/')
      }
    } catch (e) {
      console.error(e)
    }
  }
}
