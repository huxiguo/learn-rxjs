import { Observable } from "rxjs";

const myObservable = new Observable((observer) => {
    observer.next('111')
    setTimeout(() => {
        observer.next('222')
    }, 1000)
});

myObservable.subscribe((text) => {
    console.log(text)
})