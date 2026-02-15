using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Zeebe.Client;
using Zeebe.Client.Api.Responses;
using Zeebe.Client.Api.Worker;

var zeebeAddress = Environment.GetEnvironmentVariable("ZEEBE_ADDRESS") ?? "localhost:26500";
var client = ZeebeClient.Builder()
    .UseGatewayAddress(zeebeAddress)
    .UsePlainText()
    .Build();

Console.WriteLine($"✅ Подключено к Zeebe: {zeebeAddress}");
Console.WriteLine("🚀 Запуск обработчиков задач для PaymentSagaProcess...\n");

// Счётчики для эмуляции различных сценариев
int antifraudCounter = 0;
var lockObj = new object();

// 1. CREATE_PAYMENT_ORDER - создание платежного поручения и резервирование средств
RegisterWorker(client, "create-payment-order", async (jobClient, job) => {
    Console.WriteLine($"[CREATE_ORDER] Создание платежного поручения и резервирование средств (ID: {job.Key})");
    
    try
    {
        // Парсим переменные из задачи
        using var doc = JsonDocument.Parse(job.Variables);
        var paymentAmount = doc.RootElement.TryGetProperty("amount", out var amountEl) ? amountEl.GetDecimal() : 1000;
        var userId = doc.RootElement.TryGetProperty("userId", out var userIdEl) ? userIdEl.GetString() : "unknown";
        var merchantId = doc.RootElement.TryGetProperty("merchantId", out var merchantIdEl) ? merchantIdEl.GetString() : "merchant123";
        
        Console.WriteLine($"[CREATE_ORDER] Пользователь: {userId}, Сумма: {paymentAmount}, Получатель: {merchantId}");
        
        // Создаем платежное поручение
        var paymentOrderId = $"ORDER_{DateTime.Now.Ticks}";
        var reservedFundsId = $"RES_{Guid.NewGuid():N}";
        
        // Возвращаем результат с ID созданного поручения
        var result = new
        {
            paymentOrderId = paymentOrderId,
            reservedFundsId = reservedFundsId,
            orderCreated = true,
            timestamp = DateTime.UtcNow.ToString("o")
        };
        
        await jobClient.NewCompleteJobCommand(job)
            .Variables(JsonSerializer.Serialize(result))
            .Send();
            
        Console.WriteLine($"[CREATE_ORDER] ✓ Платежное поручение создано: {paymentOrderId}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[CREATE_ORDER] ✗ Ошибка: {ex.Message}");
        await jobClient.NewFailCommand(job.Key)
            .Retries(2)
            .ErrorMessage($"Ошибка создания платежного поручения: {ex.Message}")
            .Send();
    }
});

// 2. DEBIT_FUNDS - списание зарезервированных средств
RegisterWorker(client, "debit-funds", async (jobClient, job) => {
    Console.WriteLine($"[DEBIT] Списание зарезервированных средств (ID: {job.Key})");
    
    try
    {
        using var doc = JsonDocument.Parse(job.Variables);
        
        var reservedFundsId = doc.RootElement.TryGetProperty("reservedFundsId", out var reservedEl) 
            ? reservedEl.GetString() 
            : "unknown";
        var paymentOrderId = doc.RootElement.TryGetProperty("paymentOrderId", out var orderEl) 
            ? orderEl.GetString() 
            : "unknown";
        var amount = doc.RootElement.TryGetProperty("amount", out var amountEl) 
            ? amountEl.GetDecimal() 
            : 1000;
        
        Console.WriteLine($"[DEBIT] Списание: {amount} по резерву: {reservedFundsId}, заказ: {paymentOrderId}");
        
        // Имитация успешного списания (всегда успешно для примера)
        var result = new
        {
            debitTransactionId = $"DEBIT_{DateTime.Now.Ticks}",
            debitConfirmed = true,
            debitTimestamp = DateTime.UtcNow.ToString("o")
        };
        
        await jobClient.NewCompleteJobCommand(job)
            .Variables(JsonSerializer.Serialize(result))
            .Send();
            
        Console.WriteLine($"[DEBIT] ✓ Средства списаны успешно");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[DEBIT] ✗ Ошибка: {ex.Message}");
        await jobClient.NewFailCommand(job.Key)
            .Retries(3)
            .ErrorMessage($"Ошибка списания: {ex.Message}")
            .Send();
    }
});

// 3. ANTIFRAUD_CHECK - проверка на мошенничество
RegisterWorker(client, "antifraud-check", async (jobClient, job) => {
    lock (lockObj) antifraudCounter++;
    
    Console.WriteLine($"[ANTIFRAUD] Проверка на мошенничество # {antifraudCounter} (ID: {job.Key})");
    
    try
    {
        // Эмуляция различных результатов для демонстрации всех путей процесса
        // Можно менять логику в зависимости от счета или других параметров
        string fraudResult;
        
        // Для демонстрации всех трех вариантов:
        // - APPROVED (успех) - 1-й вызов
        // - REJECTED (отказ) - 2-й вызов  
        // - MANUAL_REVIEW (ручная проверка) - 3-й и далее
        switch (antifraudCounter)
        {
            case 1:
                fraudResult = "APPROVED";
                break;
            case 2:
                fraudResult = "REJECTED";
                break;
            default:
                fraudResult = "MANUAL_REVIEW";
                break;
        }
        
        Console.WriteLine($"[ANTIFRAUD] Результат проверки: {fraudResult}");
        
        using var doc = JsonDocument.Parse(job.Variables);
        var amount = doc.RootElement.TryGetProperty("amount", out var amountEl) 
            ? amountEl.GetDecimal() 
            : 1000;
        
        var result = new
        {
            fraudResult = fraudResult,
            fraudCheckId = $"FRAUD_{DateTime.Now.Ticks}",
            fraudScore = fraudResult == "APPROVED" ? 10 : (fraudResult == "REJECTED" ? 95 : 65),
            fraudDetails = fraudResult == "REJECTED" ? "Подозрительная транзакция" : "Требуется проверка",
            amount = amount
        };
        
        await jobClient.NewCompleteJobCommand(job)
            .Variables(JsonSerializer.Serialize(result))
            .Send();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ANTIFRAUD] ✗ Ошибка: {ex.Message}");
        await jobClient.NewFailCommand(job.Key)
            .Retries(2)
            .ErrorMessage($"Ошибка антифрод проверки: {ex.Message}")
            .Send();
    }
});

// 4. WAIT_FOR_MANUAL_REVIEW - ожидание ручной проверки (с таймаутом 20 мин)
RegisterWorker(client, "wait-manual-review", async (jobClient, job) => {
    Console.WriteLine($"[MANUAL_REVIEW] Ожидание ручной проверки (макс 20 мин) (ID: {job.Key})");
    
    try
    {
        using var doc = JsonDocument.Parse(job.Variables);
        var fraudCheckId = doc.RootElement.TryGetProperty("fraudCheckId", out var fraudEl) 
            ? fraudEl.GetString() 
            : "unknown";
        var amount = doc.RootElement.TryGetProperty("amount", out var amountEl) 
            ? amountEl.GetDecimal() 
            : 1000;
        
        Console.WriteLine($"[MANUAL_REVIEW] Проверка: {fraudCheckId}, Сумма: {amount}");
        
        // Здесь в реальном приложении был бы опрос внешней системы или ожидание сообщения
        // Для демонстрации - эмулируем ручное решение через небольшую задержку
        await Task.Delay(2000); // Имитация времени на проверку
        
        // Для демонстрации чередуем результаты: APPROVED, REJECTED, APPROVED, ...
        var isApproved = (antifraudCounter % 2) == 0;
        var manualDecision = isApproved ? "APPROVED" : "REJECTED";
        
        Console.WriteLine($"[MANUAL_REVIEW] Решение принято: {manualDecision}");
        
        var result = new
        {
            manualDecision = manualDecision,
            manualReviewer = "operator_" + (antifraudCounter % 3 + 1),
            manualReviewTimestamp = DateTime.UtcNow.ToString("o"),
            manualComments = isApproved ? "Транзакция одобрена оператором" : "Отклонено по подозрению в мошенничестве"
        };
        
        await jobClient.NewCompleteJobCommand(job)
            .Variables(JsonSerializer.Serialize(result))
            .Send();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[MANUAL_REVIEW] ✗ Ошибка: {ex.Message}");
        await jobClient.NewFailCommand(job.Key)
            .Retries(1)
            .ErrorMessage($"Ошибка ручной проверки: {ex.Message}")
            .Send();
    }
});

// 5. TRANSFER_TO_MERCHANT - перевод средств мерчанту
RegisterWorker(client, "transfer-to-merchant", async (jobClient, job) => {
    Console.WriteLine($"[TRANSFER] Перевод средств мерчанту (ID: {job.Key})");
    
    try
    {
        using var doc = JsonDocument.Parse(job.Variables);
        
        var amount = doc.RootElement.TryGetProperty("amount", out var amountEl) 
            ? amountEl.GetDecimal() 
            : 1000;
        var merchantId = doc.RootElement.TryGetProperty("merchantId", out var merchantEl) 
            ? merchantEl.GetString() 
            : "merchant123";
        var paymentOrderId = doc.RootElement.TryGetProperty("paymentOrderId", out var orderEl) 
            ? orderEl.GetString() 
            : "unknown";
        
        Console.WriteLine($"[TRANSFER] Перевод {amount} мерчанту {merchantId}, заказ: {paymentOrderId}");
        
        // В реальном приложении здесь был бы вызов платежного шлюза
        // Для демонстрации - всегда успешно
        var transferId = $"TR_{DateTime.Now.Ticks}";
        var transactionId = $"TXN_{Guid.NewGuid():N}";
        
        var result = new
        {
            transferId = transferId,
            transactionId = transactionId,
            transferStatus = "COMPLETED",
            transferTimestamp = DateTime.UtcNow.ToString("o"),
            transferConfirmed = true
        };
        
        await jobClient.NewCompleteJobCommand(job)
            .Variables(JsonSerializer.Serialize(result))
            .Send();
            
        Console.WriteLine($"[TRANSFER] ✓ Перевод выполнен: {transferId}, транзакция: {transactionId}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[TRANSFER] ✗ Ошибка: {ex.Message}");
        await jobClient.NewFailCommand(job.Key)
            .Retries(0) // Нет повторных попыток, сразу компенсация
            .ErrorMessage($"Ошибка перевода мерчанту: {ex.Message}")
            .Send();
    }
});

// 6. REFUND_AMOUNT - возврат средств (компенсация)
RegisterWorker(client, "refund-amount", async (jobClient, job) => {
    Console.WriteLine($"[REFUND] ВОЗВРАТ СРЕДСТВ - КОМПЕНСАЦИЯ (ID: {job.Key})");
    
    try
    {
        using var doc = JsonDocument.Parse(job.Variables);
        
        var amount = doc.RootElement.TryGetProperty("amount", out var amountEl) 
            ? amountEl.GetDecimal() 
            : 1000;
        var userId = doc.RootElement.TryGetProperty("userId", out var userIdEl) 
            ? userIdEl.GetString() 
            : "unknown";
        var reservedFundsId = doc.RootElement.TryGetProperty("reservedFundsId", out var reservedEl) 
            ? reservedEl.GetString() 
            : "unknown";
        
        Console.WriteLine($"[REFUND] Возврат {amount} пользователю {userId}, резерв: {reservedFundsId}");
        
        // Имитация возврата средств
        var refundId = $"REF_{DateTime.Now.Ticks}";
        var refundTransactionId = $"REFUND_{Guid.NewGuid():N}";
        
        var result = new
        {
            refundId = refundId,
            refundTransactionId = refundTransactionId,
            refundStatus = "COMPLETED",
            refundTimestamp = DateTime.UtcNow.ToString("o"),
            compensationCompleted = true
        };
        
        await jobClient.NewCompleteJobCommand(job)
            .Variables(JsonSerializer.Serialize(result))
            .Send();
            
        Console.WriteLine($"[REFUND] ✓ Возврат выполнен: {refundId}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[REFUND] ✗ Критическая ошибка при возврате: {ex.Message}");
        // Даже при ошибке компенсации завершаем задачу, чтобы процесс мог продолжиться
        await jobClient.NewCompleteJobCommand(job)
            .Variables("{ \"refundFailed\": true, \"refundError\": \"" + ex.Message + "\" }")
            .Send();
    }
});

// 7. SEND_NOTICE - отправка уведомлений (используется для успеха и отказа)
RegisterWorker(client, "send-notice", async (jobClient, job) => {
    Console.WriteLine($"[NOTICE] Отправка уведомления клиенту (ID: {job.Key})");
    
    try
    {
        using var doc = JsonDocument.Parse(job.Variables);
        
        // Определяем тип уведомления в зависимости от контекста
        var isSuccess = job.ElementInstanceKey != null; // Упрощенно
        
        // Извлекаем данные для уведомления
        var userId = doc.RootElement.TryGetProperty("userId", out var userIdEl) 
            ? userIdEl.GetString() 
            : "unknown";
        var amount = doc.RootElement.TryGetProperty("amount", out var amountEl) 
            ? amountEl.GetDecimal() 
            : 1000;
        
        // Проверяем, успешный это сценарий или отказ
        var hasFraudReject = doc.RootElement.TryGetProperty("fraudResult", out var fraudEl) && 
                              fraudEl.GetString() == "REJECTED";
        var hasManualReject = doc.RootElement.TryGetProperty("manualDecision", out var manualEl) && 
                               manualEl.GetString() == "REJECTED";
        var hasTransfer = doc.RootElement.TryGetProperty("transferConfirmed", out var transferEl) && 
                          transferEl.GetBoolean();
        
        string notificationType;
        string message;
        
        if (hasTransfer || (doc.RootElement.TryGetProperty("fraudResult", out var fraudApprovedEl) && fraudApprovedEl.GetString() == "APPROVED"))
        {
            notificationType = "SUCCESS";
            message = $"Платеж на сумму {amount} успешно выполнен";
            Console.WriteLine($"[NOTICE] ✓ Уведомление об УСПЕХЕ для пользователя {userId}: {message}");
        }
        else if (hasFraudReject || hasManualReject)
        {
            notificationType = "FAILED";
            message = $"Платеж на сумму {amount} отклонен службой безопасности";
            Console.WriteLine($"[NOTICE] ✗ Уведомление об ОТКАЗЕ для пользователя {userId}: {message}");
        }
        else
        {
            notificationType = "INFO";
            message = $"Статус платежа на сумму {amount} требует уточнения";
            Console.WriteLine($"[NOTICE] ℹ Уведомление о статусе для пользователя {userId}: {message}");
        }
        
        var result = new
        {
            notificationId = $"NOTIFY_{DateTime.Now.Ticks}",
            notificationType = notificationType,
            notificationMessage = message,
            notificationSent = true,
            notificationTimestamp = DateTime.UtcNow.ToString("o")
        };
        
        await jobClient.NewCompleteJobCommand(job)
            .Variables(JsonSerializer.Serialize(result))
            .Send();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[NOTICE] ✗ Ошибка: {ex.Message}");
        await jobClient.NewFailCommand(job.Key)
            .Retries(3)
            .ErrorMessage($"Ошибка отправки уведомления: {ex.Message}")
            .Send();
    }
});

// Регистрация обработчика с обработкой ошибок
void RegisterWorker(IZeebeClient client, string type, Func<IJobClient, IJob, Task> handler)
{
    client.NewWorker()
        .JobType(type)
        .Handler(async (jobClient, job) => {
            try
            {
                Console.WriteLine($"\n▶️ Запуск обработчика '{type}' (Key: {job.Key})");
                await handler(jobClient, job);
                Console.WriteLine($"✅ Завершен обработчик '{type}'\n");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] {type} failed: {ex.Message}");
                try
                {
                    await jobClient.NewFailCommand(job.Key)
                        .Retries(job.Retries > 0 ? job.Retries - 1 : 0)
                        .ErrorMessage($"Handler exception: {ex.Message}")
                        .Send();
                }
                catch (Exception failEx)
                {
                    Console.WriteLine($"[ERROR] Не удалось отправить fail-команду: {failEx.Message}");
                }
            }
        })
        .MaxJobsActive(5) // Максимум 5 одновременных задач на обработчик
        .Name($"worker-{type}")
        .Timeout(TimeSpan.FromMinutes(2)) // Таймаут на выполнение задачи
        .PollingTimeout(TimeSpan.FromMilliseconds(100))
        .PollInterval(TimeSpan.FromMilliseconds(100))
        .Open();
    
    Console.WriteLine($"✅ Обработчик '{type}' зарегистрирован");
}

// Вывод информации о запущенных обработчиках
Console.WriteLine("\n📋 Зарегистрированные обработчики для PaymentSagaProcess:");
Console.WriteLine("  • create-payment-order   - создание платежного поручения");
Console.WriteLine("  • debit-funds            - списание средств");
Console.WriteLine("  • antifraud-check        - проверка на мошенничество");
Console.WriteLine("  • wait-manual-review      - ожидание ручной проверки");
Console.WriteLine("  • transfer-to-merchant    - перевод мерчанту");
Console.WriteLine("  • refund-amount           - возврат средств (компенсация)");
Console.WriteLine("  • send-notice             - отправка уведомлений\n");

// Ожидание
Console.WriteLine("💡 Сервис работает. Для остановки: Ctrl+C");
Console.WriteLine("📊 Текущие настройки демонстрации:");
Console.WriteLine("   • 1-й запуск: APPROVED (одобрение)");
Console.WriteLine("   • 2-й запуск: REJECTED (отказ)");
Console.WriteLine("   • 3+ запуски: MANUAL_REVIEW (ручная проверка)");
Console.WriteLine("   • Ручная проверка: чередование APPROVED/REJECTED\n");

Console.CancelKeyPress += (sender, e) => {
    Console.WriteLine("\n🛑 Остановка сервиса...");
    client.Dispose();
    Environment.Exit(0);
};

Thread.Sleep(Timeout.Infinite);