import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ScootersListComponent } from './scooters-list.component';

describe('ScootersListComponent', () => {
  let component: ScootersListComponent;
  let fixture: ComponentFixture<ScootersListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ScootersListComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ScootersListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
