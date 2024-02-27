import {Component, Prop, Vue} from 'vue-property-decorator'

@Component
export default class AppMenu extends Vue {
  @Prop()
  width

  @Prop()
  activator

  @Prop()
  clickSlowkeys

  @Prop()
  clickHotkeys

  @Prop({default: {}})
  content

  isOpen = false

  render() {
    return (
      this.$createElement('v-dialog', {
        scopedSlots: {activator: ({on}) => this.activator(on)},
        on: {input: (val) =>  {
          if (val && this.clickSlowkeys) {
            this.clickSlowkeys()
          }
          if (val && this.clickHotkeys) {
            this.clickHotkeys()
          }
          this.isOpen = val
        }},
        props: {
          width: this.width,
          value: this.isOpen,
          maxWidth: '80%',
          scrollable: true,
        },
      }, [
        <v-card dense >
          <v-card-text style='fontSize: 18px;'>
            {JSON.stringify(this.content)}
          </v-card-text>
        </v-card>,
      ])
    )
  }
}
