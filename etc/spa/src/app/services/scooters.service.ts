import {Injectable} from '@angular/core';
import {catchError, map, Observable, tap, throwError} from 'rxjs';
import {HttpClient, HttpErrorResponse} from '@angular/common/http';
import {Scooter} from '../dtos/scooter';
import {environment} from '../../environments/environment';
import {ToastService} from './toast.service';
import {ServerInfo} from '../dtos/server_info';


@Injectable({
  providedIn: 'root'
})
export class ScootersService {
  private BASE_URL = environment.scootersApiUrl;

  constructor(private http: HttpClient, private toastService: ToastService) {
  }

  public getServersList(): Observable<{ servers: string[], responder: ServerInfo }> {
    return this.http.get<{ servers: string[], responder: ServerInfo }>(this.BASE_URL + '/servers')
      .pipe(
        catchError(e => this.handleError('getServersList', e)),
        tap(res => this.toastService.showSuccess('Servers list fetched', res.responder)),
      );
  }

  public getScootersList(): Observable<{ scooters: Scooter[], responder: ServerInfo }> {
    return this.http.get<{ scooters: Scooter[], responder: ServerInfo }>(this.BASE_URL + '/scooters')
      .pipe(
        catchError(e => this.handleError('getScootersList', e)),
        tap(res => this.toastService.showSuccess('Scooters list fetched', res.responder)),
      );
  }

  public createScooter(id: string): Observable<Scooter> {
    let data: Scooter = {
      'id': id,
      'is_available': true,
      'total_distance': 0,
      'current_reservation_id': ''
    };
    return this.http.put<{ newScooter: Scooter, responder: ServerInfo }>(this.BASE_URL + '/scooters/' + id, data).pipe(
      catchError(e => this.handleError('createScooter', e)),
      tap(res => this.toastService.showSuccess('Scooter "' + id + '" was created', res.responder)),
      map(res => res.newScooter)
    );
  }

  public reserveScooter(scooterId: string): Observable<string> {
    return this.http.post<{
      reservation_id: string,
      responder: ServerInfo
    }>(this.BASE_URL + '/scooters/' + scooterId + '/reservations', {})
      .pipe(
        catchError(e => this.handleError('reserveScooter', e)),
        tap(a => this.toastService.showSuccess('Scooter "' + scooterId + '" was reserved, reservation id ' + a.reservation_id, a.responder)),
        map(a => a.reservation_id)
      );
  }

  public releaseScooter(scooterId: string, reservationId: string, rideDistance: number): Observable<any> {
    // TODO: decide if also want to require the reservation id
    let data = {
      scooterId: scooterId,
      reservation_id: reservationId,
      ride_distance: rideDistance
    }
    return this.http.post<{
      status: string,
      responder: ServerInfo
    }>(this.BASE_URL + '/scooters/' + scooterId + '/releases', data).pipe(
      catchError(e => this.handleError('releaseScooter', e)),
      tap(a => this.toastService.showSuccess('Scooter "' + scooterId + '" was released, reservation id ' + reservationId + ' ride distance' + rideDistance, a.responder)),
    );
  }

  private handleError(calledMethod: string, error: HttpErrorResponse) {
    let message = 'An HTTP error occurred in ' + calledMethod + ': ' + (error.error.message || error.statusText || 'unknown error');
    this.toastService.showDanger(message);
    return throwError(() => new Error(message));
  }
}
