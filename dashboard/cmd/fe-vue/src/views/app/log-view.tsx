import {Component, Vue} from 'vue-property-decorator'
import {ServerNamespace} from '@/store'
import {SET_CLOSE, SET_OPEN} from '@/store/types'

@Component
export default class LogView extends Vue {
  @ServerNamespace.Getter
  list

  @ServerNamespace.Getter
  isOpen

  render() {
    return (
      <div
        style={{position: 'absolute', bottom: '4px', width: '100%'}}
        class='d-flex justify-end'
      >
        <v-btn x-small text onclick={this.toggleDialog} id='log-view'>
          <v-icon color='error'>mdi-alert</v-icon>
          {this.list.length}
        </v-btn>
      </div>
    )
  }

  toggleDialog() {
    this.isOpen ?
      this.closeDialog() :
      this.openDialog()
  }

  @ServerNamespace.Action(SET_OPEN)
  openDialog
  @ServerNamespace.Action(SET_CLOSE)
  closeDialog
}
