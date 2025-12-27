import {Component, OnInit} from '@angular/core';
import {ScootersService} from '../../services/scooters.service';
import {map, Observable, tap} from 'rxjs';
import {Scooter} from '../../dtos/scooter';
import {AsyncPipe, NgClass, NgForOf, NgIf} from '@angular/common';
import {ServerInfo} from '../../dtos/server_info';
import {FormsModule} from '@angular/forms';

@Component({
  selector: 'app-scooters-list',
  imports: [
    AsyncPipe,
    NgForOf,
    NgClass,
    NgIf,
    FormsModule,
  ],
  templateUrl: './scooters-list.component.html',
  styleUrl: './scooters-list.component.sass'
})
export class ScootersListComponent implements OnInit {
  protected scooters!: Observable<Scooter[]>;
  protected lastResponder!: ServerInfo;
  protected newScooterName: string = '';

  constructor(
    private scootersService: ScootersService) {
  }

  public ngOnInit(): void {
    this.refresh();
  }

  protected refresh(): void {
    this.scooters = this.scootersService.getScootersList().pipe(
      tap(a => this.lastResponder = a.responder),
      map(res => res.scooters.sort((a, b) => a.id.localeCompare(b.id)))
    );
  }

  protected createScooter(): void {
    this.scootersService.createScooter(this.newScooterName)
      .subscribe(a => {
        this.newScooterName = '';
        this.refresh();
      });
  }

  protected reserveScooter(id: string): void {
    this.scootersService.reserveScooter(id)
      .subscribe(a => {
        this.refresh();
      });
  }

  protected releaseScooter(id: string, reservationId: string, rideDistance: number): void {
    this.scootersService.releaseScooter(id, reservationId, rideDistance)
      .subscribe(a => {
        this.refresh();
      });
  }

  protected readonly parseInt = parseInt;
}
