import {Injectable, TemplateRef} from '@angular/core';
import {ServerInfo} from '../dtos/server_info';

export interface Toast {
  template: TemplateRef<any>;
  classname?: string;
  delay?: number;
  context?:any;
}

@Injectable({
  providedIn: 'root'
})
export class ToastService {

  toasts: Toast[] = [];
  private templates: { [key: string]: TemplateRef<any> } = {};

  show(toast: Toast) {
    this.toasts.push(toast);
  }

  showStandard(message:string) {
    let template = this.templates['standard'];
    this.show({
      template,
      classname: 'text-light',
      delay: 10000,
      // Pass the message as context data to the template
      context: { message } // Passing message in context
    });
  }

  showSuccess(message: string, responder?:ServerInfo) {
    let template = this.templates['success'];
    this.show({
      template,
      classname: 'bg-success text-light',
      delay: 10000,
      // Pass the message as context data to the template
      context: { message, responder } // Passing message in context
    });
  }

  showDanger(message: string) {
    let template = this.templates['danger'];
    this.show({
      template,
      classname: 'bg-danger text-light',
      delay: 15000,
      // Pass the message as context data to the template
      context: { message } // Passing message in context
    });
  }

  remove(toast: Toast) {
    this.toasts = this.toasts.filter((t) => t !== toast);
  }

  clear() {
    this.toasts.splice(0, this.toasts.length);
  }

  addTemplates(templates: { standard: TemplateRef<any>; success: TemplateRef<any>; danger: TemplateRef<any> }) {
    this.templates = templates;
  }
}
