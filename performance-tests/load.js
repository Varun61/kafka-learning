import http from 'k6/http';

export const options = {
    vus: 5,
    iterations: 5
};

export default function () {

    const payload = JSON.stringify({
        item: "apple",
        quantity: 1
    });

    http.post(
        "http://localhost:8080/orders",
        payload,
        {
            headers: {
                "Content-Type": "application/json"
            }
        }
    );
}
