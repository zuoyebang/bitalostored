import {Component, Prop, Vue} from 'vue-property-decorator'

@Component
export default class AppMenu extends Vue {
  @Prop()
  width

  @Prop()
  item

  @Prop()
  activator

  @Prop({default: 'Confirm Delete'})
  title

  @Prop({default: ''})
  content

  isOpen = false

  render() {
    return (
      this.$createElement('v-dialog', {
        scopedSlots: {activator: ({on}) => this.activator(on)},
        on: {input: (val) => this.isOpen = val},
        props: {
          width: this.width,
          value: this.isOpen,
          maxWidth: '60%',
          // maxHeight: '70vh',
          // closeOnContentClick: false,
          scrollable: true,
        },
      }, [
        <v-card dense >
          <v-card-title
            style={{position: 'sticky', top: 0}}
          >
            {this.title}
          </v-card-title>
          <v-card-text>
            {this.content}
            <div style={{position: 'sticky', bottom: 0}} class={'d-flex justify-end'}>
              <v-btn small color='error' onclick={this.onClickConfirm}>confirm</v-btn>
              <v-btn small class="ml-2" text onclick={() => this.isOpen = false}>cancel</v-btn>
            </div>
          </v-card-text>
        </v-card>,
      ])
    )
  }

  onClickConfirm() {
    this.$emit('confirm', this.item)
    this.isOpen = false
  }
}
