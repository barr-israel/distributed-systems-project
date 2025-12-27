import {AfterViewInit, Component, inject, TemplateRef, ViewChild} from '@angular/core';
import {ToastService} from '../../services/toast.service';
import {NgbToast} from '@ng-bootstrap/ng-bootstrap';
import {NgIf, NgTemplateOutlet} from '@angular/common';

@Component({
  selector: 'app-toasts-container',
  imports: [
    NgbToast,
    NgTemplateOutlet,
    NgIf
  ],
  templateUrl: './toasts-container.component.html',
  styleUrl: './toasts-container.component.sass',
  host: { class: 'toast-container position-fixed top-0 end-0 p-3', style: 'z-index: 1200' },
})
export class ToastsContainerComponent implements AfterViewInit{
  toastService = inject(ToastService);

  @ViewChild('standardTpl') standardTemplate!: TemplateRef<any>;
  @ViewChild('successTpl') successTemplate!: TemplateRef<any>;
  @ViewChild('dangerTpl') dangerTemplate!: TemplateRef<any>;

  ngAfterViewInit() {
    // Now the templates are available and can be passed to the service
    this.toastService.addTemplates({
      standard: this.standardTemplate,
      success: this.successTemplate,
      danger: this.dangerTemplate
    });
  }
}
