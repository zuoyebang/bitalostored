import {Component, Vue} from 'vue-property-decorator'
import {ServerNamespace} from '@/store'
import {ServerItem} from '@/interfaces/commons'
import {SET_CLOSE, SET_OPEN} from '@/store/types'

@Component
export default class LogDialog extends Vue {
  @ServerNamespace.Getter
  list

  @ServerNamespace.Getter
  isOpen

  activator = null

  openIndex = -1

  mounted() {
    this.activator = document.getElementById('log-view')
  }

  render() {
    return (
      <v-menu
        transition="slide-y-reverse-transition"
        oninput={this.closeDialog}
        activator={this.activator}
        value={this.isOpen}
        open-on-click={false}
        // close-on-click={false}
        close-on-content-click={false}
        max-height={'50vh'}
        top
        offset-y
      >
        <v-card class='log-dialog'>
          {this.list.map((i: ServerItem, index) => (
            <v-card-text class={'text-no-wrap' + (index !== 0 && ' mt-0 pt-0')}>
              <div>
                <b>URL:</b> {i.url}
              </div>
              <div>
                <b>ERROR:</b> {i.msg}
              </div>
              <div>
                {i.time.toLocaleTimeString()}
              </div>
              {this.openIndex === index ?
                <div>
                  <v-btn x-small text onclick={this.closeDetail}>hide</v-btn>
                  <pre>
                      {JSON.stringify(i.data, null, 2)}
                    </pre>
                </div> :
                <v-btn color='primary' x-small text onclick={() => this.openDetail(index)}>detail</v-btn>}
              {index !== this.list.length - 1 && <v-divider class={'mt-2'}/>}
            </v-card-text>
          ))}
        </v-card>
      </v-menu>
    )
  }

  closeDetail() {
    this.openIndex = -1
    if (this.list.length < 4) {
      this.closeDialog()
      this.$nextTick(() => this.openDialog())
    }
  }

  openDetail(index: number) {
    this.openIndex = index
    if (this.list.length < 4) {
      this.closeDialog()
      this.$nextTick(() => this.openDialog())
    }
  }

  @ServerNamespace.Action(SET_CLOSE)
  closeDialog
  @ServerNamespace.Action(SET_OPEN)
  openDialog
}
