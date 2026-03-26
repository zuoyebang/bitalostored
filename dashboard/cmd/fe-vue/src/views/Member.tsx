import {Vue, Component} from 'vue-property-decorator'
import {addMember, delMember, getList} from '@/api/member'
import {ROLE_LIST, ROLE_MAPPER} from '@/constant'
import AppMenu from '@/components/app-menu'

@Component({components: {AppMenu}})
export default class Member extends Vue {
  list = []

  member = {
    username: '',
    password: '',
    role: ROLE_LIST[0].value,
  }

  loading = {
    member: false,
  }

  render() {
    return (
      <v-container fluid>
        <v-card>
          <v-card-title>Member</v-card-title>
          <v-card-text>
            {this.genMemberControl()}
            {this.genMemberTable()}
          </v-card-text>
        </v-card>
      </v-container>
    )
  }

  created() {
    this.getMemberList()
  }

  async getMemberList() {
    const {data: {data}} = await getList()
    this.list = data
  }

  get canAddMember() {
    const {role, password, username} = this.member
    return role !== 0 && password.length && username.length
  }

  async addMember() {
    this.loading.member = true
    try {
      await addMember(this.member)
      this.getMemberList()
      this.member.password = ''
      this.member.username = ''
      this.member.role = 0
    } catch (e) {
      console.error(e)
    } finally {
      this.loading.member = false
    }
  }

  async deleteMember(item) {
    const {data: {data}} = await delMember(item)
    if (data === 'OK') {
      this.list = this.list.filter(({username}) => username !== item.username)
    }
  }

  genMemberControl() {
    const {username, password, role} = this.member
    return <v-row dense={true} class={'align-center mb-2'}>
      <v-flex class={'pr-2'} shrink={true}>
        <v-btn
          color={'primary'}
          disabled={!this.canAddMember}
          onclick={this.addMember}
          loading={this.loading.member}
        >new user
        </v-btn>
      </v-flex>
      <v-flex class={'pr-2'}>
        <v-text-field
          dense={true}
          hide-details={true}
          label={'Username'}
          value={username}
          oninput={(val: string) => this.member.username = val}
          outlined={true}
        />
      </v-flex>
      <v-flex class={'pr-2'}>
        <v-text-field
          dense={true}
          hide-details={true}
          label={'Password'}
          value={password}
          oninput={(val: string) => this.member.password = val}
          outlined={true}
        />
      </v-flex>
      <v-flex>
        <v-select
          dense={true}
          hide-details={true}
          label={'Role'}
          value={role}
          oninput={(val: number) => this.member.role = val}
          outlined={true}
          items={ROLE_LIST}
        />
      </v-flex>
    </v-row>
  }

  genMemberTable() {
    return (
      <v-simple-table dense={true}>
        <thead>
        <tr>
          <th>username</th>
          <th>role</th>
          <th>desc</th>
          <th>action</th>
        </tr>
        </thead>
        <tbody>
        {
          this.list.map((i) => (
            <tr>
              <td>{i.username}</td>
              <td>{i.role}</td>
              <td>{ROLE_MAPPER[i.role]}</td>
              <td>
                <app-menu
                  width={250}
                  content={<p>Username: {i.username}</p>}
                  activator={(on) => (
                    <v-btn x-small={true} icon={true} onclick={on.click}>
                      <v-icon small color={'error'}>mdi-delete</v-icon>
                    </v-btn>
                  )}
                  onconfirm={() => this.deleteMember(i)}
                />
              </td>
            </tr>
          ))
        }
        </tbody>
      </v-simple-table>
    )
  }
}
