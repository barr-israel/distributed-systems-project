import {Component, OnInit} from '@angular/core';
import {ScootersService} from '../../services/scooters.service';
import {BehaviorSubject} from 'rxjs';
import {AsyncPipe, NgForOf} from '@angular/common';
import {ToastService} from '../../services/toast.service';

@Component({
  selector: 'app-servers-list',
  imports: [
    AsyncPipe,
    NgForOf
  ],
  templateUrl: './servers-list.component.html',
  styleUrl: './servers-list.component.sass'
})
export class ServersListComponent implements OnInit {
  protected servers: BehaviorSubject<string[]> = new BehaviorSubject<string[]>([]);

  constructor(
    private scootersService: ScootersService,
    private toastService: ToastService) {
  }

  public ngOnInit(): void {
    this.refresh();
  }

  protected refresh(): void {
    this.scootersService.getServersList().subscribe(a => {
      this.servers.next(a.servers);
    });

  }
}
