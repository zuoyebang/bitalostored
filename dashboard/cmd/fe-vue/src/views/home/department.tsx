import {Component, Prop, Vue} from 'vue-property-decorator'
import AppMenu from '@/components/app-menu'
import { bindDepartment } from '@/api'


@Component({components: {AppMenu}})
export default class Department extends Vue {

  content: ''
  async bindDep() {
    await bindDepartment(this.content);
    window.location.reload()
  }
  render() {
    return (
      <app-menu
        title={'bind department'}
        onconfirm={this.bindDep}
        content={
          <v-text-field v-model={this.content}></v-text-field>
        }
        activator = {on=>
          <v-btn
            style="marginLeft: 15px"
            color={'primary'}
            onclick={on.click}
          >
            bind department
          </v-btn>
        }
      >

      </app-menu>
    )
  }
}
