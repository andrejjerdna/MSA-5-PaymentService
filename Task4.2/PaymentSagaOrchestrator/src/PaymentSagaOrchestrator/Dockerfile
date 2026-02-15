# Stage 1: Runtime base
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
WORKDIR /app

# Stage 2: Build
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src

COPY ["PaymentSagaOrchestrator.csproj", "./"]
RUN dotnet restore "PaymentSagaOrchestrator.csproj"

COPY . .
RUN dotnet build "PaymentSagaOrchestrator.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "PaymentSagaOrchestrator.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "PaymentSagaOrchestrator.dll"]