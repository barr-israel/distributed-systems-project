import {Component} from '@angular/core';
import {RouterLink, RouterLinkActive, RouterOutlet} from '@angular/router';
import {ToastsContainerComponent} from "./components/toasts-container/toasts-container.component";
import {ToastService} from './services/toast.service';
import {NgbTooltip} from '@ng-bootstrap/ng-bootstrap';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, RouterLink, RouterLinkActive, ToastsContainerComponent, NgbTooltip],
  templateUrl: './app.component.html',
  styleUrl: './app.component.sass'
})
export class AppComponent {
  title = 'CityScooter';

  constructor(private toastsService: ToastService) {
  }

  protected clearToasts(): void {
    this.toastsService.clear();
  }
}
